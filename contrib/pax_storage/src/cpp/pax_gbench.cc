#include <benchmark/benchmark.h>

static void example_benchmark(benchmark::State &state) {
  for (auto _ : state) {
  }
}
BENCHMARK(example_benchmark);

BENCHMARK_MAIN();