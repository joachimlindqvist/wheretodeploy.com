package main

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/persistent-storage", benchPersistentStorage)

	if err := http.ListenAndServe(":5555", mux); err != nil {
		log.Fatalln(err)
	}
}

func benchPersistentStorage(w http.ResponseWriter, r *http.Request) {
	type Response struct {
		TinyRWPerSecond   DiskResult
		SmallRWPerSecond  DiskResult
		MediumRWPerSecond DiskResult
		LargeRWPerSecond  DiskResult
		HugeRWPerSecond   DiskResult
	}

	var response Response

	w.Header().Add("content-type", "application/json")

	tinyRWPerSecond, err := writeFilesInSizeRangeToDir(os.TempDir(), 100000, SizeRange{128, 1024})
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		return
	}
	response.TinyRWPerSecond = *tinyRWPerSecond

	smallRWPerSecond, err := writeFilesInSizeRangeToDir(os.TempDir(), 10000, SizeRange{1024, 1024 * 1024})
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		return
	}
	response.SmallRWPerSecond = *smallRWPerSecond

	mediumRWPerSecond, err := writeFilesInSizeRangeToDir(os.TempDir(), 1000, SizeRange{1024 * 1024, 16 * 1024 * 1024})
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		return
	}
	response.MediumRWPerSecond = *mediumRWPerSecond

	largeRWPerSecond, err := writeFilesInSizeRangeToDir(os.TempDir(), 100, SizeRange{16 * 1024 * 1024, 128 * 1024 * 1024})
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		return
	}
	response.LargeRWPerSecond = *largeRWPerSecond

	hugeRWPerSecond, err := writeFilesInSizeRangeToDir(os.TempDir(), 10, SizeRange{128 * 1024 * 1024, 1024 * 1024 * 1024})
	if err != nil {
		fmt.Println(err)
		w.WriteHeader(500)
		return
	}
	response.HugeRWPerSecond = *hugeRWPerSecond

	if b, err := json.Marshal(response); err != nil {
		w.WriteHeader(500)
		return
	} else {
		w.Write(b)
		return
	}
}

type SizeRange struct {
	min int
	max int
}

type DiskResult struct {
	Seconds float32
	Count   int
	Bytes   int64
}

func writeFilesInSizeRangeToDir(dir string, count int, sizeRange SizeRange) (*DiskResult, error) {
	tempDir := os.TempDir()
	var srcFiles []string
	srcFilesCount := 10

	buf := make([]byte, 1024)

	for i := range srcFilesCount {
		if f, err := os.CreateTemp(tempDir, "small_file_src_*"); err != nil {
			return nil, fmt.Errorf("create temp file: %w", err)
		} else {
			written := 0
			maxSize := sizeRange.min + int(float32(sizeRange.max-sizeRange.min)*(float32(i)/float32(srcFilesCount)))
			for written < maxSize {
				if _, err := crand.Read(buf); err != nil {
					f.Close()
					return nil, fmt.Errorf("random bytes: %w", err)
				} else {
					maxRead := min(1024, maxSize-written)
					if w, err := f.Write(buf[0:maxRead]); err != nil {
						f.Close()
						return nil, fmt.Errorf("write to temp file: %w", err)
					} else {
						written += w
					}
				}
			}

			srcFiles = append(srcFiles, f.Name())
			f.Close()
		}
	}

	start := time.Now()

	totalWritten := int64(0)

	for i := range count {
		ii := i
		src := srcFiles[ii%len(srcFiles)]
		srcf, err := os.Open(src)
		if err != nil {
			return nil, fmt.Errorf("open src file: %w", err)
		}

		destf, err := os.CreateTemp(dir, "small_file_dest_*")
		if err != nil {
			srcf.Close()
			return nil, fmt.Errorf("open dest file: %w", err)
		}

		w, err := io.Copy(destf, srcf)
		srcf.Close()
		destf.Close()
		if err := os.Remove(destf.Name()); err != nil {
			panic(err)
		}

		if err != nil {
			return nil, fmt.Errorf("copy file: %w", err)
		} else {
			totalWritten += w
		}
	}

	since := float32(time.Since(start)) / float32(time.Second)

	for _, name := range srcFiles {
		if err := os.Remove(name); err != nil {
			return nil, fmt.Errorf("remote src files: %w", err)
		}
	}

	return &DiskResult{
		Seconds: since,
		Count:   count,
		Bytes:   totalWritten,
	}, nil
}
