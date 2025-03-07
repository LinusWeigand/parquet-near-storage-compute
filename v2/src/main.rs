use arrow2::io::parquet::read::{read_metadata_async, FileMetaData};
use bytes::Bytes;
use core::str;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use parquet2::{
    schema::types::PhysicalType,
    statistics::{BinaryStatistics, BooleanStatistics, PrimitiveStatistics},
};
use std::{env, error::Error, i64, io::SeekFrom, path::PathBuf, process::exit};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    time::Instant,
};
use tokio_util::compat::TokioAsyncReadCompatExt;

#[derive(Debug)]
struct ColumnChunkMeta {
    offset: u32,
    length: u32,
    min: u64,
    max: u64,
}

#[derive(Debug)]
struct FileMeta {
    columns: u32,
    row_groups: u32,
    col_types: Vec<ColumnType>,
    col_meta: Vec<ColumnChunkMeta>,
    meta_offset: u32,
    meta_length: u32,
}

#[derive(Debug)]
enum ColumnType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
}

#[derive(Copy, Clone)]
struct ReadChunk {
    offset: u32,
    length: u32,
}

const BLOCK_SIZE: u32 = 512 * 1024;
const TASK_COUNT: usize = 16;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    let mut folder = "/mnt/raid0";
    // let mut folder = "./merged";
    let files_per_task: usize = 40;
    let mut iter = args.iter().skip(1);
    let mut workload: usize = 0;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-p" | "--path" => {
                if let Some(v) = iter.next() {
                    folder = v;
                } else {
                    eprintln!("Error: -p/--path requires an argument.");
                    exit(1);
                }
            }
            "-w" | "--workload" => {
                if let Some(v) = iter.next() {
                    workload = match v.as_str() {
                        "best-case" => 1728380044800,
                        "real" => 151194126688,
                        "10" => 104472013456,
                        "25" => 88067753616,
                        "50" => 61551663240,
                        "75" => 48337893395,
                        "worst" => 0,
                        _ => {
                            eprintln!("Error: Invalid argument for -w/--workload.");
                            exit(1);
                        }
                    }
                } else {
                    eprintln!("Error: -w/--workload requires an argument.");
                    exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", arg);
                exit(1);
            }
        }
    }

    println!("Reading metadata...");
    let file_meta_futures = (0..TASK_COUNT)
        .map(|task_index| async move {
            let mut file_paths = Vec::new();
            for i in 0..files_per_task {
                file_paths.push(PathBuf::from(format!(
                    "{}/{}.parquet",
                    folder,
                    i + (task_index * files_per_task)
                )));
            }
            get_all_file_metadata(file_paths).await
        })
        .collect::<Vec<_>>();

    let file_meta = futures::future::try_join_all(file_meta_futures).await?;

    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        malloc_trim(0);
    }

    println!("Starting Benchmark...");
    let mut tasks = Vec::new();
    for task_file_meta in file_meta {
        let task = tokio::task::spawn(async move {
            let mut total_bytes_read = 0;
            let mut total_secs = 0.;
            let mut counter = 0;
            for (file_meta, file_path) in task_file_meta {
                counter += 1;
                let (bytes_read, secs) =
                    read_filtered_parquet_file(file_path, file_meta, workload).await?;
                if counter != 1 && counter != files_per_task {
                    total_bytes_read += bytes_read;
                    total_secs += secs;
                }
            }
            Ok::<(usize, f64), Box<dyn Error + Send + Sync>>((total_bytes_read, total_secs))
        });
        tasks.push(task);
    }

    let mut bytes_read = 0;
    let mut secs = 0.;
    for t in tasks {
        let (total_bytes_read, total_secs) = t.await??;
        bytes_read += total_bytes_read;
        secs += total_secs;
    }

    let avg_secs_per_thread = secs / TASK_COUNT as f64;
    let bytes_read_gb: f64 = bytes_read as f64 / 1000. / 1000. / 1000.;
    println!("Bytes read: {:?} GB", bytes_read_gb);

    println!("Total Time: {:.2}s", avg_secs_per_thread);

    let files_read = TASK_COUNT * (files_per_task - 2);
    let size = 0.675924377 * files_read as f64;
    let total_tp = size / avg_secs_per_thread;
    println!("Total Throughput: {:02} GB", total_tp);

    let logical_tp = bytes_read_gb / secs;
    println!("Logical Throughput: {:02} GB", logical_tp);

    println!("Selectivity: {}%", bytes_read_gb / size);
    println!("--------------------------");

    Ok(())
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

async fn get_all_file_metadata(
    file_paths: Vec<PathBuf>,
) -> Result<Vec<(FileMeta, PathBuf)>, Box<dyn Error + Send + Sync>> {
    let concurrency = file_paths.len();

    let results: Vec<(FileMeta, PathBuf)> =
        futures::stream::iter(file_paths.into_iter().map(|path| {
            tokio::spawn(async move {
                let meta = get_file_metadata(&path).await?;
                Ok::<(FileMeta, PathBuf), Box<dyn Error + Send + Sync>>((meta, path))
            })
        }))
        .buffer_unordered(concurrency)
        .then(|res: Result<_, _>| async move {
            match res {
                Ok(inner_result) => inner_result,
                Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
            }
        })
        .try_collect::<Vec<(FileMeta, PathBuf)>>()
        .await?;

    Ok(results)
}

async fn get_file_metadata(file_path: &PathBuf) -> Result<FileMeta, Box<dyn Error + Send + Sync>> {
    let mut file = File::open(file_path).await?;

    let file_length = file.seek(SeekFrom::End(0)).await?;
    if file_length < 8 {
        return Err(format!("File too small to be valid parquet: {:?}", file_path).into());
    }
    file.seek(SeekFrom::End(-8)).await?;
    let mut trailer = [0u8; 8];
    file.read_exact(&mut trailer).await?;

    let magic = &trailer[4..];
    if magic != b"PAR1" {
        return Err(format!("Invalid Parquet file magic in {:?}", file_path).into());
    }
    let meta_length = u32::from_le_bytes(trailer[0..4].try_into().unwrap());
    let meta_offset: u32 = file_length
        .checked_sub(8 + meta_length as u64)
        .ok_or_else(|| format!("metadata_len too large in {:?}", file_path))?
        as u32;

    let mut buf_reader = BufReader::new(file).compat();
    let metadata: FileMetaData = read_metadata_async(&mut buf_reader).await?;

    let columns = metadata.row_groups[0].columns().len();
    let row_groups = metadata.row_groups.len();

    let mut col_types: Vec<ColumnType> = Vec::with_capacity(columns);

    let mut col_meta = Vec::new();

    let mut first = true;
    for row_group in &metadata.row_groups {
        for column in row_group.columns() {
            let offset: u32 = column.metadata().data_page_offset as u32;
            let length: u32 = column.metadata().total_compressed_size as u32;

            let physical_type = column.descriptor().descriptor.primitive_type.physical_type;
            let stats = match column.statistics() {
                Some(Ok(v)) => v,
                _ => return Err(format!("No statistics in {:?}", file_path).into()),
            };

            let (col_type, min, max) = match physical_type {
                PhysicalType::Boolean => {
                    let stats = stats.as_any().downcast_ref::<BooleanStatistics>().unwrap();
                    let min = stats.min_value.unwrap();
                    let max = stats.max_value.unwrap();
                    (
                        ColumnType::Boolean,
                        encode_boolean(min),
                        encode_boolean(max),
                    )
                }
                PhysicalType::Int32 => {
                    let stats = stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<i32>>()
                        .unwrap();
                    let min = stats.min_value.unwrap();
                    let max = stats.max_value.unwrap();
                    (ColumnType::Int32, encode_i32(min), encode_i32(max))
                }
                PhysicalType::Int64 => {
                    let stats = stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<i64>>()
                        .unwrap();
                    let min = stats.min_value.unwrap();
                    let max = stats.max_value.unwrap();
                    (ColumnType::Int64, encode_i64(min), encode_i64(max))
                }
                PhysicalType::Float => {
                    let stats = stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<f32>>()
                        .unwrap();
                    let min = stats.min_value.unwrap();
                    let max = stats.max_value.unwrap();
                    (ColumnType::Float, encode_f32(min), encode_f32(max))
                }
                PhysicalType::Double => {
                    let stats = stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<f64>>()
                        .unwrap();
                    let min = stats.min_value.unwrap();
                    let max = stats.max_value.unwrap();
                    (ColumnType::Double, encode_f64(min), encode_f64(max))
                }
                PhysicalType::ByteArray => {
                    let stats = stats.as_any().downcast_ref::<BinaryStatistics>().unwrap();
                    let min = stats.min_value.clone().unwrap();
                    let max = stats.max_value.clone().unwrap();
                    (
                        ColumnType::ByteArray,
                        encode_byte_array(min.as_slice()),
                        encode_byte_array(max.as_slice()),
                    )
                }
                PhysicalType::Int96 => {
                    let stats = stats
                        .as_any()
                        .downcast_ref::<PrimitiveStatistics<[u32; 3]>>()
                        .unwrap();
                    let min = stats.min_value.unwrap();
                    let max = stats.max_value.unwrap();
                    (ColumnType::Int96, encode_i96(min), encode_i96(max))
                }
                _ => return Err(format!("Unsupported column type in {:?}", file_path).into()),
            };

            if first {
                col_types.push(col_type);
            }
            col_meta.push(ColumnChunkMeta {
                offset,
                length,
                min,
                max,
            });
        }
        first = false;
    }
    drop(metadata);
    let file_metadata = FileMeta {
        columns: columns as u32,
        row_groups: row_groups as u32,
        col_types,
        col_meta,
        meta_offset,
        meta_length: meta_length.into(),
    };

    Ok(file_metadata)
}

fn encode_boolean(value: bool) -> u64 {
    value.into()
}

fn decode_boolean(encoded: u64) -> bool {
    encoded != 0
}

fn encode_i32(value: i32) -> u64 {
    value as i64 as u64
}

fn decode_i32(encoded: u64) -> i32 {
    encoded as i32
}

fn encode_i64(value: i64) -> u64 {
    value as u64
}

fn decode_i64(encoded: u64) -> i64 {
    encoded as i64
}

fn encode_f32(value: f32) -> u64 {
    value.to_bits() as u64
}

fn decode_f32(encoded: u64) -> f32 {
    f32::from_bits(encoded as u32)
}

fn encode_f64(value: f64) -> u64 {
    value.to_bits()
}

fn decode_f64(encoded: u64) -> f64 {
    f64::from_bits(encoded)
}

fn encode_byte_array(value: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let len = value.len().min(8);
    buf[..len].copy_from_slice(&value[..len]);
    u64::from_le_bytes(buf)
}
fn decode_byte_array(encoded: u64) -> [u8; 8] {
    encoded.to_le_bytes()
}

fn encode_i96(value: [u32; 3]) -> u64 {
    let low = value[0] as u64;
    let mid = (value[1] as u64) << 32;
    low | mid
}

fn decode_i96(encoded: u64) -> i64 {
    let low = encoded as i32 as i64;
    let mid = ((encoded >> 32) as i32 as i64) << 32;
    low | mid
}

async fn read_filtered_parquet_file(
    file_path: PathBuf,
    file_metadata: FileMeta,
    workload: usize,
) -> Result<(usize, f64), Box<dyn Error + Send + Sync>> {
    let start = Instant::now();
    let expression = parse_expression(&format!("92 > {}", workload))?;
    let filtered_metadata = filter_metadata(&expression, file_metadata);
    let mut read_chunks = Vec::new();
    for col_meta in filtered_metadata.col_meta {
        read_chunks.push(ReadChunk {
            offset: col_meta.offset,
            length: col_meta.length,
        });
    }
    read_chunks.push(ReadChunk {
        offset: filtered_metadata.meta_offset,
        length: filtered_metadata.meta_length,
    });

    let file = File::open(file_path).await?;
    let reader = BufReader::with_capacity(BLOCK_SIZE as usize, file);

    let chunks = merge_chunks(read_chunks); // same as before

    let stream = stream::unfold(
        (reader, chunks, 0usize, 0u64, vec![0u8; BLOCK_SIZE as usize]),
        |(mut reader, chunks, mut chunk_index, mut offset_in_chunk, mut buffer)| async move {
            #[cfg(all(target_os = "linux", target_env = "gnu"))]
            unsafe {
                malloc_trim(0);
            }
            if chunk_index >= chunks.len() {
                return None;
            }
            let chunk = &chunks[chunk_index];

            if offset_in_chunk == 0 {
                if let Err(e) = reader.seek(SeekFrom::Start(chunk.offset as u64)).await {
                    return Some((
                        Err(e),
                        (reader, chunks, chunk_index, offset_in_chunk, buffer),
                    ));
                }
            }

            let chunk_remaining = chunk.length.saturating_sub(offset_in_chunk as u32);
            if chunk_remaining == 0 {
                chunk_index += 1;
                offset_in_chunk = 0;
                if chunk_index >= chunks.len() {
                    return None;
                }
                let next_chunk = &chunks[chunk_index];
                if let Err(e) = reader.seek(SeekFrom::Start(next_chunk.offset as u64)).await {
                    return Some((
                        Err(e),
                        (reader, chunks, chunk_index, offset_in_chunk, buffer),
                    ));
                }
            }

            if chunk_index >= chunks.len() {
                return None;
            }
            let chunk = &chunks[chunk_index];
            let chunk_remaining = chunk.length.saturating_sub(offset_in_chunk as u32);

            let to_read = std::cmp::min(chunk_remaining, BLOCK_SIZE) as usize;
            let slice = &mut buffer[..to_read];
            match reader.read_exact(slice).await {
                Ok(_) => {
                    offset_in_chunk += to_read as u64;
                    let bytes = Bytes::copy_from_slice(slice);
                    Some((
                        Ok(bytes),
                        (reader, chunks, chunk_index, offset_in_chunk, buffer),
                    ))
                }
                Err(e) => Some((
                    Err(e),
                    (reader, chunks, chunk_index, offset_in_chunk, buffer),
                )),
            }
        },
    );

    pin_mut!(stream);

    let mut bytes_read_real = 0;
    while let Some(item) = stream.next().await {
        match item {
            Ok(bytes) => {
                bytes_read_real += bytes.len();
            }
            Err(e) => {
                eprintln!("Error reading chunk: {}", e);
            }
        }
    }

    let secs = start.elapsed().as_millis() as f64 / 1000.;
    Ok((bytes_read_real, secs))
}

fn merge_chunks(mut chunks: Vec<ReadChunk>) -> Vec<ReadChunk> {
    if chunks.is_empty() {
        return chunks;
    }

    chunks.sort_by_key(|chunk| chunk.offset);
    let mut merged = Vec::new();
    let mut current = chunks[0];

    for chunk in chunks.into_iter().skip(1) {
        if chunk.offset <= current.offset + current.length {
            current.length =
                std::cmp::max(current.length, chunk.offset + chunk.length - current.offset);
        } else {
            merged.push(current);
            current = chunk;
        }
    }
    merged.push(current);
    merged
}

fn filter_metadata(expression: &Expression, mut file_metadata: FileMeta) -> FileMeta {
    let num_groups = file_metadata.row_groups as usize;
    let mut row_groups: Vec<Vec<ColumnChunkMeta>> = (0..num_groups).map(|_| Vec::new()).collect();

    file_metadata
        .col_meta
        .into_iter()
        .enumerate()
        .for_each(|(i, meta)| {
            row_groups[i / file_metadata.columns as usize].push(meta);
        });

    row_groups = row_groups
        .into_iter()
        .filter_map(|meta| {
            match keep_row_group(expression, &meta, &file_metadata.col_types, false) {
                true => Some(meta),
                false => None,
            }
        })
        .collect();

    file_metadata.row_groups = row_groups.len() as u32;
    file_metadata.col_meta = row_groups.into_iter().flatten().collect();
    file_metadata
}

fn keep_row_group(
    expression: &Expression,
    col_meta: &Vec<ColumnChunkMeta>,
    col_types: &Vec<ColumnType>,
    not: bool,
) -> bool {
    match &expression {
        &Expression::Condition(condition) => {
            let column_index = condition.column_index as usize;
            let meta = &col_meta[column_index];

            match &col_types[column_index] {
                &ColumnType::Int32 => {
                    let min = ThresholdValue::Int64(decode_i32(meta.min) as i64);
                    let max = ThresholdValue::Int64(decode_i32(meta.max) as i64);
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
                &ColumnType::Int64 => {
                    let min = ThresholdValue::Int64(decode_i64(meta.min));
                    let max = ThresholdValue::Int64(decode_i64(meta.max));
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
                &ColumnType::Boolean => {
                    let min = ThresholdValue::Boolean(decode_boolean(meta.min));
                    let max = ThresholdValue::Boolean(decode_boolean(meta.max));
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
                &ColumnType::Float => {
                    let min = ThresholdValue::Float64(decode_f32(meta.min) as f64);
                    let max = ThresholdValue::Float64(decode_f32(meta.max) as f64);
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
                &ColumnType::Double => {
                    let min = ThresholdValue::Float64(decode_f64(meta.min));
                    let max = ThresholdValue::Float64(decode_f64(meta.max));
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
                &ColumnType::ByteArray => {
                    let min =
                        ThresholdValue::Utf8String(bytes_to_string(decode_byte_array(meta.min)));
                    let max =
                        ThresholdValue::Utf8String(bytes_to_string(decode_byte_array(meta.max)));
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
                &ColumnType::Int96 => {
                    let min = ThresholdValue::Int64(decode_i96(meta.min));
                    let max = ThresholdValue::Int64(decode_i96(meta.max));
                    condition
                        .comparison
                        .keep_row_group(&min, &max, &condition.threshold, not)
                }
            }
        }
        &Expression::And(left, right) => match not {
            true => {
                keep_row_group(left, col_meta, col_types, true)
                    || keep_row_group(right, col_meta, col_types, true)
            }
            false => {
                keep_row_group(left, col_meta, col_types, false)
                    && keep_row_group(right, col_meta, col_types, false)
            }
        },
        &Expression::Or(left, right) => match not {
            true => {
                keep_row_group(left, col_meta, col_types, true)
                    && keep_row_group(right, col_meta, col_types, true)
            }
            false => {
                keep_row_group(left, col_meta, col_types, false)
                    || keep_row_group(right, col_meta, col_types, false)
            }
        },
        &Expression::Not(inner) => keep_row_group(inner, col_meta, col_types, !not),
    }
}

pub fn tokenize(input: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for c in input.chars() {
        match c {
            '(' | ')' | ' ' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                if c != ' ' {
                    tokens.push(c.to_string());
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

pub fn parse_expression(input: &str) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let tokens = tokenize(input)?;
    let mut pos = 0;
    parse_or(&tokens, &mut pos)
}

pub fn parse_or(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let mut expr = parse_and(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "OR" {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        expr = Expression::Or(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_and(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let mut expr = parse_not(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "AND" {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        expr = Expression::And(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_not(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    if *pos < tokens.len() && tokens[*pos] == "NOT" {
        *pos += 1;
        let expr = parse_primary(tokens, pos)?;
        return Ok(Expression::Not(Box::new(expr)));
    }

    parse_primary(tokens, pos)
}

pub fn parse_primary(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    if *pos >= tokens.len() {
        return Err("Unexpected end of input".into());
    }

    if tokens[*pos] == "(" {
        *pos += 1;
        let expr = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != ")" {
            return Err("Expected closing parenthesis".into());
        }
        *pos += 1;
        return Ok(expr);
    }

    // Parse condition
    let column_index = tokens[*pos].parse().unwrap();
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected comparison operator".into());
    }

    let comparison = Comparison::from_str(&tokens[*pos]).ok_or("Invalid comparison operator")?;
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected threshold value".into());
    }

    let threshold_token = &tokens[*pos];
    *pos += 1;

    let threshold = if let Ok(bool) = threshold_token.parse::<bool>() {
        ThresholdValue::Boolean(bool)
    } else if threshold_token.contains('.') {
        if let Ok(num) = threshold_token.parse::<f64>() {
            ThresholdValue::Float64(num)
        } else {
            ThresholdValue::Utf8String(threshold_token.to_owned())
        }
    } else if let Ok(num) = threshold_token.parse::<i64>() {
        ThresholdValue::Int64(num)
    } else {
        ThresholdValue::Utf8String(threshold_token.to_owned())
    };

    Ok(Expression::Condition(Condition {
        column_index,
        comparison,
        threshold,
    }))
}

#[derive(Debug)]
pub struct Condition {
    pub column_index: u64,
    pub threshold: ThresholdValue,
    pub comparison: Comparison,
}

#[derive(Debug)]
pub enum ThresholdValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8String(String),
}

#[derive(Debug)]
pub enum Expression {
    Condition(Condition),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

#[derive(PartialEq, Debug)]
pub enum Comparison {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
}

impl Comparison {
    pub fn from_str(input: &str) -> Option<Self> {
        match input {
            "<" => Some(Comparison::LessThan),
            "<=" => Some(Comparison::LessThanOrEqual),
            "==" => Some(Comparison::Equal),
            ">=" => Some(Comparison::GreaterThanOrEqual),
            ">" => Some(Comparison::GreaterThan),
            _ => None,
        }
    }
}
impl Comparison {
    pub fn keep_row_group(
        &self,
        row_group_min: &ThresholdValue,
        row_group_max: &ThresholdValue,
        user_threshold: &ThresholdValue,
        not: bool,
    ) -> bool {
        match (row_group_min, row_group_max, user_threshold) {
            (ThresholdValue::Int64(min), ThresholdValue::Int64(max), ThresholdValue::Int64(v)) => {
                compare(min, max, v, self, not)
            }
            (
                ThresholdValue::Float64(min),
                ThresholdValue::Float64(max),
                ThresholdValue::Float64(v),
            ) => compare_floats(*min, *max, *v, self, not),
            (
                ThresholdValue::Boolean(min),
                ThresholdValue::Boolean(max),
                ThresholdValue::Boolean(v),
            ) => match self {
                Comparison::LessThan => true,
                Comparison::LessThanOrEqual => true,
                Comparison::Equal => match not {
                    false => v == min || v == max,
                    true => !(v == min && v == max),
                },
                Comparison::GreaterThanOrEqual => true,
                Comparison::GreaterThan => true,
            },
            (
                ThresholdValue::Utf8String(min),
                ThresholdValue::Utf8String(max),
                ThresholdValue::Utf8String(v),
            ) => compare(min, max, v, self, not),
            _ => true,
        }
    }
}

pub fn compare<T: Ord>(min: T, max: T, v: T, comparison: &Comparison, not: bool) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v == min && v == max),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}

pub fn compare_floats<T: Float>(min: T, max: T, v: T, comparison: &Comparison, not: bool) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v.equal(min) && v.equal(max)),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}
pub trait Float: Copy + PartialOrd {
    fn abs(self) -> Self;
    fn equal(self, other: Self) -> bool;
}

impl Float for f32 {
    fn abs(self) -> Self {
        self.abs()
    }
    fn equal(self, other: Self) -> bool {
        (self - other).abs() < f32::EPSILON
    }
}

impl Float for f64 {
    fn abs(self) -> Self {
        self.abs()
    }
    fn equal(self, other: Self) -> bool {
        (self - other).abs() < f64::EPSILON
    }
}
fn bytes_to_string(bytes: [u8; 8]) -> String {
    match str::from_utf8(&bytes) {
        Ok(s) => s.trim_end_matches('\0').to_string(),
        Err(_) => String::from_utf8_lossy(&bytes).to_string(),
    }
}
