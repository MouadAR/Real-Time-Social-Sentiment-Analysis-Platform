import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")  # Set log level to ERROR to reduce console output

    # Creating a streaming context with batch interval of 10 sec
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")  # Provide directory for checkpointing
    pwords = load_wordlist("./Dataset/positive.txt")
    nwords = load_wordlist("./Dataset/negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)

def make_plot(counts):
    """
    This function plots the counts of positive and negative words for each timestep.
    """
    positiveCounts = []
    negativeCounts = []
    time = []

    for val in counts:
        positiveTuple = val[0]
        positiveCounts.append(positiveTuple[1])
        negativeTuple = val[1]
        negativeCounts.append(negativeTuple[1])

    for i in range(len(counts)):
        time.append(i)

    plt.plot(time, positiveCounts, 'bo-', label='Positive')
    plt.plot(time, negativeCounts, 'go-', label='Negative')
    plt.axis([0, len(counts), 0, max(max(positiveCounts), max(negativeCounts)) + 50])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc='upper left')
    plt.show()

def load_wordlist(filename):
    """
    This function returns a list or set of words from the given filename.
    """
    words = set()
    with open(filename, 'r') as f:
        for line in f:
            words.add(line.strip())
    return words

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount) 

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics=['twitter_stream'], kafkaParams={"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].decode("ascii", "ignore"))

    # Each element of tweets will be the text of a tweet.
    words = tweets.flatMap(lambda line: line.split(" "))
    positive = words.map(lambda word: ('Positive', 1) if word in pwords else ('Positive', 0))
    negative = words.map(lambda word: ('Negative', 1) if word in nwords else ('Negative', 0))
    allSentiments = positive.union(negative)
    sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)
    runningSentimentCounts = sentimentCounts.updateStateByKey(updateFunction)
    runningSentimentCounts.pprint()

    # The counts variable holds the word counts for all time steps
    counts = []
    sentimentCounts.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    # Start the computation
    ssc.start()
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGracefully=True)

    return counts

if __name__ == "__main__":
    main()
