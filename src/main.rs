use std::sync::mpsc;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom},
    thread::{self},
};

// Number of threads that will be spawned to process chunks
const THREADS: u64 = 11;
const MEASUREMENT_FILE: &'static str = "measurements.txt";
const TOTAL_CITIES: usize = 10_000;
const BUFF_CAPACITY: usize = 2097152;

// Result from each thread
struct ComputeResult {
    pub consumed_lines: u64,
    pub consumed_bytes: usize,
    pub weather_data: WeatherHashMap,
}

struct ChunkData {
    pub start: u64,
    pub end: u64,
    pub chunk_size: u64,
}

// The chunk_file function divides a file into smaller, nearly equal parts for multiple threads to
// process in parallel, ensuring each chunk has a defined start and end position.
// If only one thread is used, the entire file is treated as a single chunk.
fn chunk_file(file_size: u64, threads: u64) -> Vec<ChunkData> {
    if threads == 0 || threads == 1 {
        return vec![ChunkData {
            start: 0,
            chunk_size: file_size,
            end: file_size - 1,
        }];
    }

    let round_off = threads - (file_size % threads);

    let rounded_size = file_size + round_off;

    let chunk_size = rounded_size / threads;

    (0..threads)
        .into_iter()
        .map(|n| {
            let start = n * chunk_size;

            let (end, chunk_size) = if n == threads - 1 {
                // For the last chunk, set the end to the actual file size - 1.
                // This avoids overshooting the file size by using the rounded size.
                (file_size - 1, file_size - start)
            } else {
                (start + chunk_size - 1, chunk_size)
            };

            ChunkData {
                start,
                chunk_size,
                end,
            }
        })
        .collect()
}

// This function adjusts the starting positions of the file chunks so that each chunk begins at a
// newline character, ensuring clean line breaks. This prevents data overlap and misalignment
// between chunks when splitting measurements across multiple threads.
fn correct_chunk_position<F: Read + Seek>(file: &mut F, chunks: &mut Vec<ChunkData>, threads: u64) {
    const NEW_LINE: u8 = b'\n';

    for thread_num in 0..threads {
        if thread_num == 0 {
            continue;
        }

        let current = chunks.get_mut(thread_num as usize).unwrap();

        if let Err(_) = file.seek(SeekFrom::Start(current.start - 1)) {
            return;
        }

        let mut file_buf: [u8; 108] = [0; 108];

        file.read_exact(&mut file_buf).unwrap();

        if file_buf[1] == NEW_LINE {
            current.chunk_size -= 1;
            current.start += 1;

            let previous = chunks.get_mut((thread_num - 1) as usize).unwrap();
            previous.chunk_size += 1;
            previous.end += 1;
        } else if file_buf[0] == NEW_LINE {
            // ok
        } else {
            let mut current_index = 2;

            loop {
                if file_buf[current_index] == NEW_LINE {
                    current.start += current_index as u64;
                    current.chunk_size = current.end - current.start + 1;

                    let previous = chunks.get_mut((thread_num - 1) as usize).unwrap();
                    previous.end += current_index as u64;
                    previous.chunk_size = previous.end - previous.start + 1;

                    break;
                }

                current_index += 1;

                if current_index == 107 {
                    return;
                }
            }
        }
    }
}

fn string_to_float(s: &[u8]) -> f32 {
    let (rest, sign) = if s[0] == b'-' {
        (&s[1..], -1.0_f32)
    } else {
        (s, 1.0_f32)
    };

    let mut value = (rest[0] - b'0') as f32;

    if rest[1] != b'.' {
        value = value * 10.0 + (rest[1] - b'0') as f32;
        value += (rest[3] - b'0') as f32 / 10.0;
    } else {
        value += (rest[2] - b'0') as f32 / 10.0;
    }

    value * sign
}

struct StationEntry {
    min: f32,
    sum: f32,
    max: f32,
    count: f32,
    // We can use String, but the performance is comparable to using a static buffer.
    name: [u8; 100],
    // Why u8? Because we know that the city name will be 100 bytes max.
    len: u8,
}

impl StationEntry {
    const fn new() -> Self {
        Self {
            min: 0.0,
            sum: 0.0,
            max: 0.0,
            count: 0.0,
            name: [0u8; 100],
            len: 0,
        }
    }

    fn name_string(&self) -> &str {
        unsafe { core::str::from_utf8_unchecked(&self.name[0..self.len as usize]) }
    }
}

struct WeatherHashMap {
    pub data: [StationEntry; TOTAL_CITIES],
    pub occupied_indexes: Vec<usize>,
}

impl WeatherHashMap {
    pub fn new() -> Self {
        Self {
            data: [const { StationEntry::new() }; TOTAL_CITIES],
            occupied_indexes: Vec::new(),
        }
    }

    pub fn entry(&mut self, city: &[u8], reading: f32) {
        let mut index = WeatherHashMap::hash(city);

        let city_len = city.len();

        loop {
            let entry = unsafe { self.data.get_unchecked_mut(index) };
            if entry.len == city_len as u8 && &entry.name[..city_len as usize] == city {
                entry.min = entry.min.min(reading);
                entry.max = entry.max.max(reading);

                entry.sum += reading;
                entry.count += 1.0;
                break;
            }

            if entry.len == 0 {
                entry.name[..city_len as usize].copy_from_slice(city);
                entry.len = city_len as u8;
                entry.min = reading;
                entry.sum = reading;
                entry.max = reading;
                entry.count = 1.0;

                self.occupied_indexes.push(index);

                break;
            }

            index += 1;
        }
    }

    #[cfg(target_os = "macos")]
    fn hash(city: &[u8]) -> usize {
        let mut crc = 0;

        for c in city {
            crc = unsafe { std::arch::aarch64::__crc32cb(crc, *c) };
        }

        // To keep the hash in between 0 and 9,999 which is our index range.
        crc as usize % (TOTAL_CITIES - 1)
    }

    #[cfg(not(target_os = "macos"))]
    fn hash(city: &[u8]) -> usize {
        let mut h = std::hash::DefaultHasher::new();

        std::hash::Hasher::write(&mut h, city);

        std::hash::Hasher::finish(&h) as usize % (TOTAL_CITIES - 1)
    }
}

fn main() {
    let meta = fs::metadata(MEASUREMENT_FILE).unwrap();

    let mut chunks = chunk_file(meta.len(), THREADS);

    let mut file = OpenOptions::new()
        .read(true)
        .open(MEASUREMENT_FILE)
        .unwrap();

    correct_chunk_position(&mut file, &mut chunks, THREADS);

    let mut handles = vec![];

    let (tx, rx) = mpsc::channel::<ComputeResult>();

    for chunk in chunks {
        let t = thread::Builder::new()
            .stack_size((BUFF_CAPACITY * 2) + TOTAL_CITIES + (1024 * 1024 * 8));

        let tx = tx.clone();

        handles.push(
            t.spawn(move || {
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(MEASUREMENT_FILE)
                    .unwrap();

                let mut buffer = [0u8; BUFF_CAPACITY];

                let mut weather_data = WeatherHashMap::new();

                let mut consumed_bytes = 0;

                let mut consumed_lines = 0;

                let mut file_read_at = chunk.start;

                file.seek(SeekFrom::Start(file_read_at)).unwrap();

                'outer: loop {
                    file.read(&mut buffer).unwrap();

                    let mut last = 0;

                    let mut idx = 5;

                    while idx < BUFF_CAPACITY {
                        if buffer[idx] != b'\n' {
                            idx += 1;
                            continue;
                        }

                        let str_buffer = &buffer[last..=idx];

                        let entry_len = str_buffer.len();

                        let mut delim_idx = entry_len - 5;

                        loop {
                            if str_buffer[delim_idx] == b';' {
                                break;
                            }
                            delim_idx -= 1;
                        }

                        let temp = string_to_float(&str_buffer[(delim_idx + 1)..(entry_len - 1)]);

                        weather_data.entry(&str_buffer[0..delim_idx], temp);

                        consumed_bytes += entry_len;

                        consumed_lines += 1;

                        if consumed_bytes == chunk.chunk_size as usize {
                            break 'outer;
                        }

                        last = idx + 1;

                        idx += 6;
                    }

                    file_read_at = file_read_at + last as u64;

                    file.seek(SeekFrom::Start(file_read_at)).unwrap();
                }

                tx.send(ComputeResult {
                    consumed_bytes,
                    consumed_lines,
                    weather_data,
                })
                .unwrap();
            })
            .unwrap(),
        );
    }

    let mut consumed_bytes = 0;
    let mut consumed_lines = 0;
    let mut weather_data = HashMap::<String, (f32, f32, f32, f32)>::new();

    let mut received_count = 0;
    while let Ok(compute) = rx.recv() {
        received_count += 1;

        consumed_bytes += compute.consumed_bytes;
        consumed_lines += compute.consumed_lines;
        for idx in compute.weather_data.occupied_indexes {
            let station_entry = &compute.weather_data.data[idx];

            let city = station_entry.name_string();
            if let Some(entry) = weather_data.get_mut(city) {
                if station_entry.min < entry.0 {
                    entry.0 = station_entry.min;
                }

                if station_entry.max > entry.2 {
                    entry.2 = station_entry.max;
                }
                entry.1 += station_entry.sum;
                entry.3 += station_entry.count;
            } else {
                weather_data.insert(
                    city.to_string(),
                    (
                        station_entry.min,
                        station_entry.sum,
                        station_entry.max,
                        station_entry.count,
                    ),
                );
            }
        }

        if received_count == THREADS {
            break;
        }
    }

    let mut final_data = vec![];

    for (city, data) in weather_data {
        final_data.push((city, data.0, data.1 / data.3, data.2));
    }

    final_data.sort_by_key(|i| i.0.to_lowercase());

    let mut result = "{".to_string();

    for data in final_data {
        result.push_str(&data.0);
        result.push_str("=");
        result.push_str(&format!("{:.1}/{:.1}/{:.1}, ", data.1, data.2, data.3));
    }

    result = result.trim_end_matches(", ").to_string();

    result.push_str("}");

    println!("{result}");

    println!();
    println!("Consumed lines: {consumed_lines}");
    println!("Consumed bytes: {consumed_bytes}");
}
