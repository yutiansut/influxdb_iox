//! This module contains the IOx implementation for using Google Cloud Storage
//! as the object store.
use crate::{
    path::{cloud::CloudConverter, ObjectStorePath},
    DataDoesNotMatchLength, Result, UnableToDeleteDataFromGcs, UnableToDeleteDataFromGcs2,
    UnableToGetDataFromGcs, UnableToGetDataFromGcs2, UnableToListDataFromGcs,
    UnableToListDataFromGcs2, UnableToPutDataToGcs,
};
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use snafu::{ensure, ResultExt};
use std::io;

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    bucket_name: String,
}

impl GoogleCloudStorage {
    /// Configure a connection to Google Cloud Storage.
    pub fn new(bucket_name: impl Into<String>) -> Self {
        Self {
            bucket_name: bucket_name.into(),
        }
    }

    /// Save the provided bytes to the specified location.
    pub async fn put<S>(&self, location: &ObjectStorePath, bytes: S, length: usize) -> Result<()>
    where
        S: Stream<Item = io::Result<Bytes>> + Send + Sync + 'static,
    {
        let temporary_non_streaming = bytes
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .expect("Should have been able to collect streaming data")
            .to_vec();

        ensure!(
            temporary_non_streaming.len() == length,
            DataDoesNotMatchLength {
                actual: temporary_non_streaming.len(),
                expected: length,
            }
        );

        let location = CloudConverter::convert(&location);
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        let _ = tokio::task::spawn_blocking(move || {
            cloud_storage::Object::create(
                &bucket_name,
                &temporary_non_streaming,
                &location_copy,
                "application/octet-stream",
            )
        })
        .await
        .context(UnableToPutDataToGcs {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(())
    }

    /// Return the bytes that are stored at the specified location.
    pub async fn get(
        &self,
        location: &ObjectStorePath,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let location = CloudConverter::convert(&location);
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        let bytes = tokio::task::spawn_blocking(move || {
            cloud_storage::Object::download(&bucket_name, &location_copy)
        })
        .await
        .context(UnableToGetDataFromGcs {
            bucket: &self.bucket_name,
            location: location.clone(),
        })?
        .context(UnableToGetDataFromGcs2 {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(futures::stream::once(async move { Ok(bytes.into()) }))
    }

    /// Delete the object at the specified location.
    pub async fn delete(&self, location: &ObjectStorePath) -> Result<()> {
        let location = CloudConverter::convert(&location);
        let location_copy = location.clone();
        let bucket_name = self.bucket_name.clone();

        tokio::task::spawn_blocking(move || {
            cloud_storage::Object::delete(&bucket_name, &location_copy)
        })
        .await
        .context(UnableToDeleteDataFromGcs {
            bucket: &self.bucket_name,
            location: location.clone(),
        })?
        .context(UnableToDeleteDataFromGcs2 {
            bucket: &self.bucket_name,
            location,
        })?;

        Ok(())
    }

    /// List all the objects with the given prefix.
    pub async fn list<'a>(
        &'a self,
        prefix: Option<&'a ObjectStorePath>,
    ) -> Result<impl Stream<Item = Result<Vec<ObjectStorePath>>> + 'a> {
        let bucket_name = self.bucket_name.clone();
        let prefix = prefix.map(CloudConverter::convert);

        let objects = tokio::task::spawn_blocking(move || match prefix {
            Some(prefix) => cloud_storage::Object::list_prefix(&bucket_name, &prefix),
            None => cloud_storage::Object::list(&bucket_name),
        })
        .await
        .context(UnableToListDataFromGcs {
            bucket: &self.bucket_name,
        })?
        .context(UnableToListDataFromGcs2 {
            bucket: &self.bucket_name,
        })?;

        Ok(futures::stream::once(async move {
            Ok(objects
                .into_iter()
                .map(|o| ObjectStorePath::from_cloud_unchecked(o.name))
                .collect())
        }))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        path::ObjectStorePath,
        tests::{get_nonexistent_object, put_get_delete_list},
        Error, GoogleCloudStorage, ObjectStore,
    };
    use bytes::Bytes;
    use std::env;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    // Helper macro to skip tests if the GCP environment variables are not set.
    // Skips become hard errors if TEST_INTEGRATION is set.
    macro_rules! maybe_skip_integration {
        () => {
            dotenv::dotenv().ok();

            let bucket_name = env::var("GCS_BUCKET_NAME");
            let force = std::env::var("TEST_INTEGRATION");

            match (bucket_name.is_ok(), force.is_ok()) {
                (false, true) => {
                    panic!("TEST_INTEGRATION is set, but GCS_BUCKET_NAME is not")
                }
                (false, false) => {
                    eprintln!("skipping integration test - set GCS_BUCKET_NAME to run");
                    return Ok(());
                }
                _ => {}
            }
        };
    }

    fn bucket_name() -> Result<String> {
        Ok(env::var("GCS_BUCKET_NAME")
            .map_err(|_| "The environment variable GCS_BUCKET_NAME must be set")?)
    }

    #[tokio::test]
    async fn gcs_test() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = bucket_name()?;

        let integration =
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));
        put_get_delete_list(&integration).await?;
        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = bucket_name()?;
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let integration =
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));

        let result = get_nonexistent_object(&integration, Some(location_name)).await?;

        assert_eq!(
            result,
            Bytes::from(format!(
                "No such object: {}/{}",
                bucket_name, NON_EXISTENT_NAME
            ))
        );

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = NON_EXISTENT_NAME;
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let integration =
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name));

        let result = get_nonexistent_object(&integration, Some(location_name)).await?;

        assert_eq!(result, Bytes::from("Not Found"));

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = bucket_name()?;
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let integration =
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(&bucket_name));

        let err = integration.delete(&location_name).await.unwrap_err();

        if let Error::UnableToDeleteDataFromGcs2 {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = NON_EXISTENT_NAME;
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let integration =
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name));

        let err = integration.delete(&location_name).await.unwrap_err();

        if let Error::UnableToDeleteDataFromGcs2 {
            source,
            bucket,
            location,
        } = err
        {
            assert!(matches!(source, cloud_storage::Error::Google(_)));
            assert_eq!(bucket, bucket_name);
            assert_eq!(location, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type")
        }

        Ok(())
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() -> Result<()> {
        maybe_skip_integration!();
        let bucket_name = NON_EXISTENT_NAME;
        let location_name = ObjectStorePath::from_cloud_unchecked(NON_EXISTENT_NAME);
        let integration =
            ObjectStore::new_google_cloud_storage(GoogleCloudStorage::new(bucket_name));
        let data = Bytes::from("arbitrary data");
        let stream_data = std::io::Result::Ok(data.clone());

        let result = integration
            .put(
                &location_name,
                futures::stream::once(async move { stream_data }),
                data.len(),
            )
            .await;
        assert!(result.is_ok());

        Ok(())
    }
}
