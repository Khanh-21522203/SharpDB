using BenchmarkDotNet.Running;
BenchmarkSwitcher.FromAssembly(typeof(SharpDB.V2.Benchmarks.BTreeBenchmarks).Assembly).Run(args);
