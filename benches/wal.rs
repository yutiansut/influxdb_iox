use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tempfile::TempDir;
use wal::{Wal, WalOptions};

fn wal1(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append-1", |b| {
        let dir = TempDir::new().unwrap();
        let wal = Wal::with_options(
            dir.path().to_path_buf(),
            WalOptions::default().sync_writes(false),
        )
        .unwrap();
        b.iter(|| wal.append(&bytes).unwrap())
    });

    group.finish();
}


fn wal10(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes((bytes.len() as u64)*10));

    group.bench_function("append-10", |b| {
        let dir = TempDir::new().unwrap();
        let wal = Wal::with_options(
            dir.path().to_path_buf(),
            WalOptions::default().sync_writes(false),
        )
        .unwrap();
        b.iter(|| {
            for _ in 0..10 {
                wal.append(&bytes).unwrap();
            }
        });
    });


    group.finish();
}


fn wal100(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes((bytes.len() as u64)*100));

    group.bench_function("append-10", |b| {
        let dir = TempDir::new().unwrap();
        let wal = Wal::with_options(
            dir.path().to_path_buf(),
            WalOptions::default().sync_writes(false),
        )
        .unwrap();
        b.iter(|| {
            for _ in 0..100 {
                wal.append(&bytes).unwrap();
            }
        });
    });


    group.finish();
}


criterion_group!(benches, wal1, wal10, wal100);

criterion_main!(benches);
