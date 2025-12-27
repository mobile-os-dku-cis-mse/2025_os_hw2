
- 44GB 크기의 파일로 측정했습니다.
- 실험 환경
    - cpu model: Intel(R) Xeon(R) Gold 5320
    - 52 threads / 26 cores
    - L1 / L2 / L3 : 1.2  / 32.5 / 39

    
- 3번 시나리오는 제가 baseline으로 잡은 시나리오입니다. 원형큐로 버퍼를 만들었습니다.
- 5번 시나리오는 락을 분리하여 여러 버퍼를 갖도록 한 시나리오입니다. 개별 버퍼는 3번 시나리오의 버퍼를 사용합니다.
- 7번 시나리오는 mmap을 사용한 시나리오입니다. 버퍼에는 포인터를 넘겨 copy를 줄인 방식입니다.
- 이것들에 대한 정리는 /resultsOfScenarios/final_performance_comparison.log에 있습니다.

| 지표 | 05_sharded | 07_mmap | 03_multi_chunk |
| --- | --- | --- | --- |
| 시간 | 02.74 | 04.25 | 20.81 |

## 실행 방법

시나리오는 최소 3개, 최대 5개의 인자를 받습니다.

파일 / producer의 개수 / consumer의 개수 / buffer 크기 / shard 개수

```text
// 시나리오 1
./01_single_line large_random_file.bin 10 10 
// 시나리오 2
./02_single_chunk large_random_file.bin 10 10 
// 시나리오 3
./03_multiple_chunk large_random_file.bin 10 10 20
// 시나리오 4
./04_detailed_profile large_random_file.bin 10 10 20
// 시나리오 5
./05_sharded large_random_file.bin 10 10 10 10
// 시나리오 6
./06_mmap large_random_file.bin 10 10 100
// 시나리오 7
./07_mmap_sharded large_random_file.bin 10 10 10 10
```