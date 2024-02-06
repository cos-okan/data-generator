package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/cos-okan/common"
)

var (
	writeMasterDataToRedis        = false
	produceRawTwrDataToRedpanda   = true
	produceMdUpdateDataToRedpanda = true
	routineCount                  = 10
	tagCount                      = 1000
	anchorCount                   = 10
	cyclePeriodInMs               = 1000
	testDurationInSec             = 1
	wg                            = sync.WaitGroup{}
)

func main() {
	if writeMasterDataToRedis {
		writeMockDataToRedis(anchorCount, tagCount)
	}

	// if produceRawTwrDataToRedpanda || produceMdUpdateDataToRedpanda {
	// 	PrepareAvroHelper()
	// }

	start := time.Now()

	if produceRawTwrDataToRedpanda {
		sendMockRawTwrDistance(routineCount, tagCount, anchorCount, cyclePeriodInMs, testDurationInSec)
	}

	if produceMdUpdateDataToRedpanda {
		sendMockMdUpdate()
	}

	wg.Wait()
	fmt.Println("Total time: ", time.Since(start).Milliseconds())
	time.Sleep(time.Second)
}

func writeMockDataToRedis(anchorCount int, tagCount int) {
	rtlsRedis := common.PrepareRedisClient("localhost:6379", "", 0, 3)
	rtlsRedis.WriteMockDataToRedis(anchorCount, tagCount)
	time.Sleep(time.Second)
}

func sendMockRawTwrDistance(rCnt int, tCnt int, aCnt int, twrPeriod int, duration int) {
	tagCountPerRoutine := tCnt / rCnt
	for i := 0; i < rCnt; i++ {
		wg.Add(1)
		tagIdStart := 10001 + (i * tagCountPerRoutine)
		go SendRawTwrDistanceData(&wg, tagCountPerRoutine, aCnt, twrPeriod, duration, tagIdStart)
	}
}

func SendRawTwrDistanceData(wg *sync.WaitGroup, tagCount int, anchorCount int, cyclePeriodInMs int, testDurationInSec int, tagIdStart int) {
	defer wg.Done()

	topic := "raw-twr"
	brokers := []string{"localhost:19092"}
	rp := common.NewRedpandaProducer(brokers, topic)

	start := time.Now()
	msgNo := 0

	cycleCount := (testDurationInSec * 1000) / cyclePeriodInMs
	waitTime := time.Duration(float64(cyclePeriodInMs/100) * float64(time.Millisecond))
	for i := 0; i < cycleCount; i++ {
		nextCycleStartTime := time.Now().Add(time.Millisecond * time.Duration(cyclePeriodInMs))
		msgNo++
		for j := 0; j < tagCount; j++ {
			twr := common.TwrDistance{
				FromNodeId:        tagIdStart + j,
				ToNodeId:          1001,
				MessageNo:         msgNo,
				Distance:          300,
				FwConfidenceLevel: 100,
				Timestamp:         time.Now().UTC(),
			}

			for k := 0; k < anchorCount; k++ {
				twr.ToNodeId = 1001 + k
				rp.SendAvroMessage(&twr)
				time.Sleep(time.Nanosecond * 10)
			}
		}
		for time.Now().Before(nextCycleStartTime) {
			time.Sleep(waitTime)
		}
	}

	fmt.Println("Batch time: ", time.Since(start).Milliseconds())
}

func sendMockMdUpdate() {
	time.Sleep(time.Second * 15)
	mdUpdate := common.MasterDataUpdate{
		Operation: 2, // delete
		DataType:  1, // anchor
		Key:       "anchor:1001",
		Timestamp: time.Now(),
	}
	topic := "master-data-update"
	brokers := []string{"localhost:19092"}
	rp := common.NewRedpandaProducer(brokers, topic)

	rp.SendAvroMessage(&mdUpdate)
	time.Sleep(time.Second * 25)

	mdUpdate = common.MasterDataUpdate{
		Operation: 1, // add
		DataType:  1, // anchor
		Key:       "anchor:1001",
		Anchor: common.Anchor{
			ID: 1001,
			Location: common.Location{
				FloorID: 1,
				Point:   common.Point{X: 0, Y: 0, Z: 250},
			},
			Range: 1000,
		},
		Timestamp: time.Now(),
	}
	rp.SendAvroMessage(&mdUpdate)
	time.Sleep(time.Second * 2)
}
