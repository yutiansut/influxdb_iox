use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tempfile::TempDir;
use wal::{Wal, WalOptions};

fn wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append", |b| {
        let dir = TempDir::new().unwrap();
        let wal = Wal::with_options(
            dir.path().to_path_buf(),
            WalOptions::default().sync_writes(false),
        )
        .unwrap();
        b.iter(|| wal.append(&bytes))
    });

    group.finish();
}

criterion_group!(benches, wal);

criterion_main!(benches);
