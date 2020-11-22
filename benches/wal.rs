use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tempfile::TempDir;
use wal::{WalBuilder, WritePayload};

fn wal1(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append-1", |b| {
        let dir = TempDir::new().unwrap();
        let mut wal = WalBuilder::new(dir.path().to_path_buf()).wal().unwrap();
        b.iter(|| {
            let payload = WritePayload::new(bytes.clone()).unwrap();
            wal.append(payload).unwrap();
        });
    });

    group.finish();
}


fn wal10(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append-10", |b| {
        let dir = TempDir::new().unwrap();
        let mut wal = WalBuilder::new(dir.path().to_path_buf()).wal().unwrap();
        b.iter(|| {
            for _ in 0..10 {
                let payload = WritePayload::new(bytes.clone()).unwrap();
                wal.append(payload).unwrap();
            }
        });
    });

    group.finish();
}


fn wal100(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal");
    let bytes = vec![1u8; 4096];

    group.throughput(Throughput::Bytes(bytes.len() as u64));

    group.bench_function("append-100", |b| {
        let dir = TempDir::new().unwrap();
        let mut wal = WalBuilder::new(dir.path().to_path_buf()).wal().unwrap();
        b.iter(|| {
            for _ in 0..100 {
                let payload = WritePayload::new(bytes.clone()).unwrap();
                wal.append(payload).unwrap();
            }
        });
    });

    group.finish();
}




criterion_group!(benches, wal1, wal10, wal100);

criterion_main!(benches);
