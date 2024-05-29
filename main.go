package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	"github.com/joho/godotenv"
)

func main() {
	// Load the .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	// Initialize a session in the us-west-2 region.
	// Replace with your region.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	)

	if err != nil {
		log.Fatalf("Failed to initialize AWS session: %v", err)
	}

	// Get the current time
	now := time.Now()
	// Create S3 service client
	svc := s3.New(sess)
	// Retrieve the credentials value
	creds, err := sess.Config.Credentials.Get()
	if err != nil {
		fmt.Println("Error getting credentials:", err)
		return
	} else {
		fmt.Println("Credentials:", creds)
	}

	bucket := "dspm-s3-test-1"
	// prefix := "your-prefix" // if you want to filter by prefix

	// Create a channel to receive S3 object keys
	objectKeys := make(chan string, 1000)

	var wg sync.WaitGroup
	// List objects in the S3 bucket
	totalSize := int64(0)

	// Worker function to process objects
	worker := func(id int, jobs <-chan string) {
		for key := range jobs {
			// do nothing
			//fmt.Printf("Worker %d processing key: %s\n", id, key)
			// Here you can add code to read the object, process it, etc.
			// For example:
			getObject(svc, bucket, key)
		}
		wg.Done()
	}

	// Start a few worker goroutines
	numWorkers := 100
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go worker(w, objectKeys)
	}

	// List objects in the S3 bucket
	err = svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	},
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				objectKeys <- *obj.Key
				totalSize += *obj.Size
			}
			return true
		})

	if err != nil {
		log.Fatalf("Failed to list objects: %v", err)
	}

	// Close the channel and wait for all workers to finish
	close(objectKeys)
	wg.Wait()
	later := time.Now()
	// Calculate the difference
	diff := later.Sub(now)
	fmt.Println("All workers finished in: ", diff)
	fmt.Printf("Total size of files in bucket '%s': %s\n", bucket, humanize.Bytes(uint64(totalSize)))

}

func getObject(svc *s3.S3, bucket, key string) {
	// Get the object from the S3 bucket
	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Printf("Failed to get object %s: %v", key, err)
		return
	}
	defer result.Body.Close()

	// // Process the object (example: read the contents)
	// buf := new(bytes.Buffer)
	// buf.ReadFrom(result.Body)
	// fmt.Printf("Contents of %s: %s\n", key, buf.String())
}
