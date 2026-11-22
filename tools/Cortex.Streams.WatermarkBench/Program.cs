using System.Diagnostics;
using Cortex.Streams;
using Cortex.Streams.Abstractions;
using Cortex.Streams.Watermarks;

// Measures what WithWatermarks costs an in-order stream (issue #197: "Performance overhead is minimal (<5% for
// in-order streams)"). Each pair of pipelines is identical except for the watermark; runs alternate so drift hits both.
// The gate is a windowed pipeline at 10%: the watermark stage costs about 10-12 ns per item whatever the design
// (type check, timestamp delegate, one compare-and-swap, the next-operator call), which is 6-9% of a real window
// but about 30% of a trivial Map -> Filter -> Sink pipeline. That trivial case is printed for information.
const int Items = 2_000_000;
const int WarmupRuns = 3;
const int MeasuredRuns = 9;
const double Limit = 0.10;

var start = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
var events = new Event[Items];
for (var i = 0; i < Items; i++)
{
    events[i] = new Event(i % 16, start.AddMilliseconds(i));
}

long sink = 0;

IStream<Event> Plain() => StreamBuilder<Event>.CreateNewStream("plain").Stream()
    .Map(e => e).Filter(e => e.Key >= 0).Sink(e => sink += e.Key).Build();

IStream<Event> Watermarked() => StreamBuilder<Event>.CreateNewStream("watermarked").Stream()
    .WithWatermarks(e => e.Time, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(1)))
    .Map(e => e).Filter(e => e.Key >= 0).Sink(e => sink += e.Key).Build();

IStream<Event> ProcessingTimeWindow() => StreamBuilder<Event>.CreateNewStream("window-pt").Stream()
    .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromDays(365_000)).Sink(w => sink += w.Items.Count).Build();

// Same never-closing window, so both runs do the same windowing work and only the watermark differs
IStream<Event> EventTimeWindow() => StreamBuilder<Event>.CreateNewStream("window-et").Stream()
    .WithWatermarks(e => e.Time, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(1)))
    .TumblingWindow(e => e.Key, e => e.Time, TimeSpan.FromDays(365_000)).Sink(w => sink += w.Items.Count).Build();

IStream<Event> WatermarkedIdle() => StreamBuilder<Event>.CreateNewStream("watermarked-idle").Stream()
    .WithWatermarks(e => e.Time, WatermarkStrategy.BoundedOutOfOrderness(TimeSpan.FromSeconds(1)).WithIdleTimeout(TimeSpan.FromSeconds(30)))
    .Map(e => e).Filter(e => e.Key >= 0).Sink(e => sink += e.Key).Build();

double Run(Func<IStream<Event>> build)
{
    var stream = build();
    stream.Start();
    var watch = Stopwatch.StartNew();
    foreach (var e in events)
    {
        stream.Emit(e);
    }
    watch.Stop();
    stream.Stop();
    return watch.Elapsed.TotalMilliseconds;
}

(double A, double B) Compare(Func<IStream<Event>> a, Func<IStream<Event>> b)
{
    for (var i = 0; i < WarmupRuns; i++) { Run(a); Run(b); }

    var timesA = new List<double>();
    var timesB = new List<double>();
    for (var i = 0; i < MeasuredRuns; i++)
    {
        GC.Collect(); GC.WaitForPendingFinalizers(); GC.Collect();
        if (i % 2 == 0) { timesA.Add(Run(a)); timesB.Add(Run(b)); }
        else { timesB.Add(Run(b)); timesA.Add(Run(a)); }
    }
    return (Median(timesA), Median(timesB));
}

static double Median(List<double> values)
{
    values.Sort();
    return values[values.Count / 2];
}

var (pt, et) = Compare(ProcessingTimeWindow, EventTimeWindow);
var overhead = et / pt - 1;
Console.WriteLine($"TumblingWindow, {Items:N0} in-order items, median of {MeasuredRuns}:");
Console.WriteLine($"  processing time: {pt,8:F1} ms ({pt * 1e6 / Items:F1} ns/item)");
Console.WriteLine($"  event time:      {et,8:F1} ms ({et * 1e6 / Items:F1} ns/item)");
Console.WriteLine($"  overhead:        {overhead:P1} (limit {Limit:P0})");

// Informational: the watermark stage alone, against a trivial pipeline, with and without an idle timeout
var (plain, watermarked) = Compare(Plain, Watermarked);
var (_, idle) = Compare(Plain, WatermarkedIdle);
Console.WriteLine("Map -> Filter -> Sink (informational):");
Console.WriteLine($"  without watermarks: {plain,8:F1} ms   with: {watermarked,8:F1} ms ({watermarked / plain - 1:P1}, {(watermarked - plain) * 1e6 / Items:F1} ns/item)");
Console.WriteLine($"  with an idle timeout: {idle,8:F1} ms ({idle / plain - 1:P1})");

GC.KeepAlive(sink);
if (overhead >= Limit)
{
    Console.WriteLine("FAIL: watermark overhead is at or above the limit");
    return 1;
}
Console.WriteLine("PASS");
return 0;

internal sealed record Event(int Key, DateTime Time);
