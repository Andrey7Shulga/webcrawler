package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final ForkJoinPool pool;
  private final PageParserFactory pageParserFactory;
  private final ReentrantLock lock = new ReentrantLock();

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls,
          PageParserFactory pageParserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.ignoredUrls = ignoredUrls;
    this.maxDepth = maxDepth;
    this.pageParserFactory = pageParserFactory;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {

    Instant deadline = clock.instant().plus(timeout);
    ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    for (String url : startingUrls) {
      pool.invoke(new MultiCrawler (url, deadline, maxDepth, counts, visitedUrls));
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }

    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }

     public class MultiCrawler extends RecursiveTask<Boolean> {

       private final String url;
       private final Instant deadline;
       private final int maxDepth;
       private final ConcurrentMap<String, Integer> counts;
       private final ConcurrentSkipListSet<String> visitedUrls;

       public MultiCrawler(String url, Instant deadline, int maxDepth, ConcurrentMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls) {
         this.url = url;
         this.deadline = deadline;
         this.maxDepth = maxDepth;
         this.counts = counts;
         this.visitedUrls = visitedUrls;
       }

       @Override
       protected Boolean compute() {
         if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
           return false;
         }

         for (Pattern pattern : ignoredUrls) {
           if (pattern.matcher(url).matches()) {
             return false;
           }
         }

         try {
           lock.lock();
           if (visitedUrls.contains(url)) {
             return false;
           }
           visitedUrls.add(url);
         } finally {
           lock.unlock();
         }

         PageParser.Result result = pageParserFactory.get(url).parse();

         for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
           counts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
         }

         List<MultiCrawler> slaveTasks = new ArrayList<>();
           for (String link : result.getLinks()) {
             slaveTasks.add(new MultiCrawler(link, deadline, maxDepth - 1, counts, visitedUrls));
           }
         invokeAll(slaveTasks);
           return true;
       }
     }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
