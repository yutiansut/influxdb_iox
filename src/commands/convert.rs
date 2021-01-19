use data_types::schema::Schema;
use influxdb_line_protocol::parse_lines;
use ingest::{
    parquet::writer::{CompressionLevel, Error as ParquetWriterError, IOxParquetTableWriter},
    ConversionSettings, Error as IngestError, LineProtocolConverter, TSMFileConverter,
};
use packers::{Error as TableError, IOxTableWriter, IOxTableWriterSource};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    convert::TryInto,
    fs,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};
use tracing::{debug, info, warn};

use crate::commands::input::{FileType, InputReader};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error reading {} ({})", name.display(), source))]
    UnableToReadInput {
        name: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "Cannot write multiple measurements to a single file. Saw new measurement named {}",
        new_measurement_name
    ))]
    MultipleMeasurementsToSingleFile { new_measurement_name: String },

    #[snafu(display("Internal error: measurement name not specified in schema",))]
    InternalMeasurementNotSpecified {},

    #[snafu(display("Error creating a parquet table writer {}", source))]
    UnableToCreateParquetTableWriter { source: ParquetWriterError },

    #[snafu(display("Conversion from Parquet format is not implemented"))]
    ParquetNotImplemented,

    #[snafu(display("Error writing remaining lines {}", source))]
    UnableToWriteGoodLines { source: IngestError },

    #[snafu(display("Error opening input {}", source))]
    OpenInput { source: super::input::Error },

    #[snafu(display("Error while closing the table writer {}", source))]
    UnableToCloseTableWriter { source: IngestError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for TableError {
    fn from(source: Error) -> Self {
        Self::from_other(source)
    }
}

/// Creates  `IOxParquetTableWriter` suitable for writing to a single file
#[derive(Debug)]
struct ParquetFileWriterSource {
    output_filename: String,
    compression_level: CompressionLevel,
    // This creator only supports  a single filename at this time
    // so track if it has alread been made, for errors
    made_file: bool,
}

impl IOxTableWriterSource for ParquetFileWriterSource {
    // Returns a `IOxTableWriter suitable for writing data from packers.
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn IOxTableWriter>, TableError> {
        let measurement = schema
            .measurement()
            .cloned()
            .context(InternalMeasurementNotSpecified)?;

        if self.made_file {
            return MultipleMeasurementsToSingleFile {
                new_measurement_name: measurement,
            }
            .fail()?;
        }

        let output_file = fs::File::create(&self.output_filename).map_err(|e| {
            TableError::from_io(
                e,
                format!("Error creating output file {}", self.output_filename),
            )
        })?;
        info!(
            "Writing output for measurement {} to {} ...",
            measurement, self.output_filename
        );

        let writer = IOxParquetTableWriter::new(schema, self.compression_level, output_file)
            .context(UnableToCreateParquetTableWriter)?;
        self.made_file = true;
        Ok(Box::new(writer))
    }
}

/// Creates `IOxParquetTableWriter` for each measurement by
/// writing each to a separate file (measurement1.parquet,
/// measurement2.parquet, etc)
#[derive(Debug)]
struct ParquetDirectoryWriterSource {
    compression_level: CompressionLevel,
    output_dir_path: PathBuf,
}

impl IOxTableWriterSource for ParquetDirectoryWriterSource {
    /// Returns a `IOxTableWriter` suitable for writing data from packers.
    /// named in the template of <measurement.parquet>
    fn next_writer(&mut self, schema: &Schema) -> Result<Box<dyn IOxTableWriter>, TableError> {
        let mut output_file_path: PathBuf = self.output_dir_path.clone();

        let measurement = schema
            .measurement()
            .context(InternalMeasurementNotSpecified)?;
        output_file_path.push(measurement);
        output_file_path.set_extension("parquet");

        let output_file = fs::File::create(&output_file_path).map_err(|e| {
            TableError::from_io(
                e,
                format!("Error creating output file {:?}", output_file_path),
            )
        })?;
        info!(
            "Writing output for measurement {} to {:?} ...",
            measurement, output_file_path
        );

        let writer = IOxParquetTableWriter::new(schema, self.compression_level, output_file)
            .context(UnableToCreateParquetTableWriter)
            .map_err(TableError::from_other)?;
        Ok(Box::new(writer))
    }
}

pub fn is_directory(p: impl AsRef<Path>) -> bool {
    fs::metadata(p)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

pub fn convert(
    input_path: &str,
    output_path: &str,
    compression_level: CompressionLevel,
) -> Result<()> {
    info!("convert starting");
    debug!("Reading from input path {}", input_path);

    if is_directory(input_path) {
        let mut files: Vec<_> = fs::read_dir(input_path)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|filename| filename.path().extension().map_or(false, |x| x == "tsm"))
            .collect();

        if files.is_empty() {
            warn!("No TSM files found");
            return Ok(());
        }

        // Sort files by their TSM generation to ensure any duplicate block
        // data is appropriately de-duplicated.
        files.sort_by_key(|a| a.file_name());

        let mut index_readers = Vec::with_capacity(files.len());
        let mut block_readers = Vec::with_capacity(files.len());
        for file in &files {
            let index_handle = File::open(file.path()).unwrap();
            let index_size = index_handle.metadata().unwrap().len();
            let block_handle = File::open(file.path()).unwrap();

            index_readers.push((BufReader::new(index_handle), index_size as usize));
            block_readers.push(BufReader::new(block_handle));
        }

        // setup writing
        let writer_source: Box<dyn IOxTableWriterSource> = if is_directory(&output_path) {
            info!("Writing to output directory {:?}", output_path);
            Box::new(ParquetDirectoryWriterSource {
                compression_level,
                output_dir_path: PathBuf::from(output_path),
            })
        } else {
            info!("Writing to output file {}", output_path);
            Box::new(ParquetFileWriterSource {
                compression_level,
                output_filename: String::from(output_path),
                made_file: false,
            })
        };

        let mut converter = TSMFileConverter::new(writer_source);
        return converter
            .convert(index_readers, block_readers)
            .context(UnableToCloseTableWriter);
    }

    let input_reader = InputReader::new(input_path).context(OpenInput)?;
    info!(
        "Preparing to convert {} bytes from {}",
        input_reader.len(),
        input_path
    );

    match input_reader.file_type() {
        FileType::LineProtocol => convert_line_protocol_to_parquet(
            input_path,
            input_reader,
            compression_level,
            output_path,
        ),
        FileType::TSM => {
            // TODO(edd): we can remove this when I figure out the best way to share
            // the reader between the TSM index reader and the Block decoder.
            let input_block_reader = InputReader::new(input_path).context(OpenInput)?;
            let len = input_reader.len() as usize;
            convert_tsm_to_parquet(
                input_reader,
                len,
                compression_level,
                input_block_reader,
                output_path,
            )
        }
        FileType::Parquet => ParquetNotImplemented.fail(),
    }
}

fn convert_line_protocol_to_parquet(
    input_filename: &str,
    mut input_reader: InputReader,
    compression_level: CompressionLevel,
    output_name: &str,
) -> Result<()> {
    // TODO: make a streaming parser that you can stream data through in blocks.
    // for now, just read the whole input at once into a string
    let mut buf = String::with_capacity(
        input_reader
            .len()
            .try_into()
            .expect("Cannot allocate buffer"),
    );
    input_reader
        .read_to_string(&mut buf)
        .context(UnableToReadInput {
            name: input_filename,
        })?;

    // FIXME: Design something sensible to do with lines that don't
    // parse rather than just dropping them on the floor
    let only_good_lines = parse_lines(&buf).filter_map(|r| match r {
        Ok(line) => Some(line),
        Err(e) => {
            warn!("Ignorning line with parse error: {}", e);
            None
        }
    });

    let writer_source: Box<dyn IOxTableWriterSource> = if is_directory(&output_name) {
        info!("Writing to output directory {:?}", output_name);
        Box::new(ParquetDirectoryWriterSource {
            compression_level,
            output_dir_path: PathBuf::from(output_name),
        })
    } else {
        info!("Writing to output file {}", output_name);
        Box::new(ParquetFileWriterSource {
            output_filename: String::from(output_name),
            compression_level,
            made_file: false,
        })
    };

    let settings = ConversionSettings::default();
    let mut converter = LineProtocolConverter::new(settings, writer_source);
    converter
        .convert(only_good_lines)
        .context(UnableToWriteGoodLines)?;
    converter.finalize().context(UnableToCloseTableWriter)?;
    info!("Completing writing to {} successfully", output_name);
    Ok(())
}

fn convert_tsm_to_parquet(
    index_stream: InputReader,
    index_stream_size: usize,
    compression_level: CompressionLevel,
    block_stream: InputReader,
    output_name: &str,
) -> Result<()> {
    // setup writing
    let writer_source: Box<dyn IOxTableWriterSource> = if is_directory(&output_name) {
        info!("Writing to output directory {:?}", output_name);
        Box::new(ParquetDirectoryWriterSource {
            compression_level,
            output_dir_path: PathBuf::from(output_name),
        })
    } else {
        info!("Writing to output file {}", output_name);
        Box::new(ParquetFileWriterSource {
            compression_level,
            output_filename: String::from(output_name),
            made_file: false,
        })
    };

    let mut converter = TSMFileConverter::new(writer_source);
    converter
        .convert(vec![(index_stream, index_stream_size)], vec![block_stream])
        .context(UnableToCloseTableWriter)
}
