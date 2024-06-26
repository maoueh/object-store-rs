package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/streamingfast/cli"
	"github.com/streamingfast/dstore"
)

func main() {
	arguments := os.Args
	if len(arguments) != 3 {
		fmt.Fprintf(os.Stderr,
			"Usage: %s <merged_blocks_store_url> <block_offset>",
			arguments[0],
		)
		os.Exit(1)
	}

	// Keep a buffer of 16KiB to read the object
	buffer := make([]byte, 1*1024)

	store, err := dstore.NewStore(arguments[1], "dbin.zst", "none", false)
	cli.NoError(err, "unable to create store")

	blockOffset := readBlockOffset(arguments[2])

	start := time.Now()
	testDuration := 120 * time.Second
	totalBytes := 0

	totalFetchTime := time.Duration(0)
	totalFetchCount := 0

	windowStart := time.Now()
	windowPeriod := 5 * time.Second
	windowBytes := 0

	fmt.Printf("Starting read test (transfer rate will be printed each %s)\n", windowPeriod)

	ctx := context.Background()
	for i := 0; 1 < 1000; i++ {
		blockNum := uint64(i*100) + blockOffset
		filename := fmt.Sprintf("%010d", blockNum)

		func() {
			openStart := time.Now()
			reader, err := store.OpenObject(ctx, filename)
			cli.NoError(err, "unable to open object")
			defer reader.Close()

			totalFetchTime += time.Since(openStart)
			totalFetchCount++

			for {
				data, err := reader.Read(buffer)
				if err == io.EOF {
					break
				}
				cli.NoError(err, "unable to read object")

				if time.Since(windowStart) > windowPeriod {
					fmt.Println(bytesRate(uint64(windowBytes), windowPeriod))
					windowStart = time.Now()
					windowBytes = 0
				}

				totalBytes += data
				windowBytes += data
			}
		}()

		if time.Since(start) > testDuration {
			break
		}
	}

	fmt.Println()
	fmt.Printf("Overall average fetch time: %s (%d fetches, %.2f%% of total time)\n",
		totalFetchTime/time.Duration(totalFetchCount),
		totalFetchCount,
		percentageOfTotalTime(totalFetchTime, testDuration),
	)
	fmt.Printf(
		"Overall transfer rate: %s (%d bytes in %s)\n",
		bytesRate(uint64(totalBytes), testDuration),
		totalBytes,
		time.Since(start),
	)
}

func percentageOfTotalTime(counter time.Duration, total time.Duration) float64 {
	return float64(counter) / float64(total) * 100
}

func readBlockOffset(in string) uint64 {
	blockOffset, err := strconv.ParseUint(in, 0, 64)
	cli.NoError(err, "unable to parse block offset")

	return blockOffset
}

func bytesRate(byteCount uint64, period time.Duration) string {
	rate := float64(byteCount) / period.Seconds()

	return fmt.Sprintf("%s/s", humanize.IBytes(uint64(rate)))
}
