//! Compare safe SIMD payload copying with the standard library's copy routine.
#![forbid(unsafe_code)]
use fearless_simd::{Level, Simd, dispatch, prelude::*};
use std::{hint::black_box, time::Instant};

#[fearless_simd_macros::simd]
fn simd_copy<S: Simd>(simd: S, source: &[u8], target: &mut [u8]) {
    let width = S::u8s::LEN;
    let mut src = source.chunks_exact(width);
    let mut dst = target.chunks_exact_mut(width);
    for (input, output) in src.by_ref().zip(dst.by_ref()) {
        S::u8s::from_slice(simd, input).store_slice(output);
    }
    dst.into_remainder().copy_from_slice(src.remainder());
}

fn main() {
    let arguments: Vec<_> = std::env::args().collect();
    let engine = arguments.get(1).map_or("scalar", String::as_str);
    let size: usize = arguments
        .get(2)
        .map_or("16384", String::as_str)
        .parse()
        .expect("size");
    assert!(size > 0 && size <= 1024 * 1024, "size must be 1..=1048576");
    let rounds = (512 * 1024 * 1024 / size).max(1);
    let source: Vec<u8> = (0..size).map(|n| n.wrapping_mul(31) as u8).collect();
    let mut destination = vec![0; size];
    let level = Level::new();
    let started = Instant::now();
    for _ in 0..rounds {
        let source = black_box(source.as_slice());
        let destination = black_box(destination.as_mut_slice());
        match engine {
            "scalar" => destination.copy_from_slice(source),
            "simd" => dispatch!(level, simd => simd_copy(simd, source, destination)),
            _ => panic!("engine must be scalar or simd"),
        }
    }
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(destination, source, "copy corrupted the frame payload");
    println!(
        "engine={engine} size={size} rounds={rounds} seconds={seconds:.6} GiB_per_sec={:.3}",
        (rounds * size) as f64 / seconds / 1024.0_f64.powi(3)
    );
}
