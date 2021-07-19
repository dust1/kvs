use std::collections::HashMap;
use std::path::{PathBuf, Path};
use serde::{Serialize, Deserialize};
use std::io::{Seek, Read, BufReader, SeekFrom, Write, BufWriter};
use std::{io, fs};
use std::fs::{File, OpenOptions, read};
use serde_json::Deserializer;
use std::ffi::OsStr;

//调用自身模块一样需要从lib.rc获取
use crate::{Result, KvsError};
use std::option::Option::Some;

//最大一个文件中最大文件大小
const COMPACTION_THRESHOLD: u64 = 128;


//KV存储定义
pub struct KvStore {
    path: PathBuf,     //文件存储目录
    //读取器修改为文件编号-读写器索引
    readers : HashMap<u64, BufReaderWithPos<File>>,
    //同一时间有且只有一个文件能被写入，因此写入器不变
    writer : BufWriterWithPos<File>,
    index : HashMap<String, CommandPos>,
    //记录当前写入文件的序号
    current_gen : u64,
    //未压缩的数据大小
    uncompacted : u64
}

//命令定义，对于写入来说有两种操作：set、rm
#[derive(Serialize, Deserialize, Debug)]
pub enum Command {
    Set {key : String, value : String},
    Remove {key : String},
}

//数据索引，包含命令所在的偏移量(pos)与长度(len)
struct CommandPos {
    pos : u64,      //命令所在偏移位置
    len : u64,      //命令长度
    gen : u64       //命令所在文件编号
}

//带寻址读取功能的读取器
struct BufReaderWithPos<R: Read + Seek> {
    reader : BufReader<R>,
    pos : u64
}

//带寻址读取功能的写入器
struct BufWriterWithPos<W: Write + Seek> {
    writer : BufWriter<W>,
    pos : u64
}


///KV存储实现
impl KvStore {

    ///根据传入path打开对应的KvStore
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let mut readers = HashMap::<u64, BufReaderWithPos<File>>::new();
        let mut index = HashMap::<String, CommandPos>::new();

        let gen_list = sort_gen_list(&path)?;
        let mut uncompacted = 0;
        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(load_path(&path, gen))?)?;
            uncompacted += load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }
        //新的文件编号为文件列表个数+1，如果为空则默认为0
        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        let writer = new_log_file(&path, current_gen, &mut readers)?;

        Ok(KvStore {
            path,
            readers,
            writer,
            index,
            current_gen,
            uncompacted
        })
    }

    ///写入
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        //获取当前写入器的指针所在的偏移位置
        //为何不是写入器对应文件的长度？
        //文件会有一部分覆盖写，实际数据不一定等于文件长度
        let pos = self.writer.pos;

        //将cmd转化为json并写入writer中
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;

        //if let控制流，详情见：
        //https://kaisery.github.io/trpl-zh-cn/ch06-03-if-let.html
        if let Command::Set {key, .. } = cmd {
            //创建索引对象
            //此时self.writer.pos已经是写入完成后所在的偏移位置
            let cmd_pos = CommandPos {pos, len: self.writer.pos - pos,  gen: self.current_gen};
            //加入索引，后面还需要获取索引对象,并计算到压缩数据大小
            if let Some(old_cmd) = self.index.insert(key, cmd_pos) {
                self.uncompacted += old_cmd.len;
            }
            // if self.uncompacted > COMPACTION_THRESHOLD {}
        }
        if self.uncompacted > COMPACTION_THRESHOLD {
            //若当前文件数据大小超过限定大小，执行压缩
            self.compact()?
        }
        Ok(())
    }

    ///查询,如果数据不存在则返回None
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self.readers.get_mut(&cmd_pos.gen)
                .expect(format!("不存在的文件编号{}", cmd_pos.gen).as_str());
            //移动到数据所在位置
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;

            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set {value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    ///删除
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::Remove {key};
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush();
            if let Command::Remove {key } = cmd {
                self.index.remove(&key).expect("key not found");
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    ///压缩
    pub fn compact(&mut self) -> Result<()> {
        //下一个序号为压缩结果
        let compaction_gen = self.current_gen + 1;
        //下下一个序号为新写入文件，同时修改写入对象
        self.current_gen += 2;
        //修改写入器
        self.writer = self.new_log_file(self.current_gen)?;

        let mut new_pos = 0;
        //根据压缩的目标文件编号创建写入器，并将其加入读取器
        let mut compaction_writer = self.new_log_file(compaction_gen)?;
        //遍历当前索引的key
        for cmd_pos in &mut self.index.values_mut() {
            //获取当前key的关联文件读取器
            let reader = self.readers.get_mut(&cmd_pos.gen)
                .expect(format!("无法找到读取器的文件编号: {}", &cmd_pos.gen).as_str());
            //将读取器的游标切换到命令的起始位置
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            //设置读取器读取的数据长度
            let mut cmd_reader = reader.take(cmd_pos.len);
            //把命令拷贝到压缩日志写入器中
            let len = io::copy(&mut cmd_reader, &mut compaction_writer)?;
            //更新索引中key的命令位置数据
            *cmd_pos = CommandPos {gen: compaction_gen, pos: new_pos, len };
            new_pos += len;
        }
        compaction_writer.flush()?;

        //日志序号是递增的，只需要保留最大的序号的文件即可
        let stale_gens: Vec<_> = self.readers.keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned().collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.uncompacted = 0;
        Ok(())
    }

    ///将写入器定位到新的文件编号
    fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, gen, &mut self.readers)
    }

}


//写入命令的实现
impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set {key, value}
    }
    fn remove(key: String) -> Command {
        Command::Remove {key}
    }
}


///读取器实现
impl <R:Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner:R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader : BufReader::new(inner),
            pos,
        })
    }
}

//读取器不同接口的时间代码体不同，这样不会使得不同接口的代码都在一个构造体中出现混乱
///读取器对Read的实现
impl <R:Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

///对Seek的实现
impl <R:Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

///写入器实现
impl <W:Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner:W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPos {
            writer : BufWriter::new(inner),
            pos,
        })
    }
}

impl <R:Write + Seek> Write for BufWriterWithPos<R> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl <R:Write + Seek> Seek for BufWriterWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

///根据path返回这个文件夹目录下的所有文件编号，返回的编号要排序
fn sort_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list : Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten().collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

///根据文件编号、读取器加载数据并将其放入内存索引中。
fn load(gen: u64, reader:&mut BufReaderWithPos<File>,index: &mut HashMap<String, CommandPos>) -> Result<u64> {
    //将reader的指针移动到起点0，即pos开始为0
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    //根据Command数据序列化格式从reader中读取并反序列化为Command对象,真牛逼
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

    //压缩后可以保存的字节数据
    let mut uncompacted = 0;
    //遍历stream，将结果包装在Some中，
    //Some中的值必然不为空，即let Some(cmd) = stream.next()
    //这段语句会先判断strean.next()返回的值是否为空
    //如果不为空则boolean判断为true，并创建Some(cmd)对象
    //如果为空则boolean判断为false，则跳出循环
    while let Some(cmd) = stream.next() {
        //获取读取一次Command后reader的指针所在的位置
        //这个指针指向下一个Command的初始位置，因此为new_pos
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set {key, ..} => {
                //将Set命令在内存中重现，如果添加成功则返回添加成功的对象
                //并将对象的长度添加到uncompacted中
                //？为什么不直接用cmd对象呢？
                //需要将cmd的所有权交给内存索引
                if let Some(old_cmd) = index.insert(key, CommandPos {gen, pos, len : new_pos - pos}) {
                    uncompacted += old_cmd.len;
                }
            },
            Command::Remove {key, ..} => {
                //复现Remove命令，将对应的key从索引中删除
                if let Some(old_cmd) = index.remove(&key) {
                    //将Remove的长度添加到字节数据长度中
                    uncompacted += old_cmd.len;
                }
                //?
                uncompacted += new_pos - pos;
            }
        }
        pos = new_pos;
    }
    Ok(uncompacted)
}

///根据存储文件夹路径与文件编号获取该文件编号对应的存储文件
fn load_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}

///根据path与文件编号创建新的日志文件，并将其加入readers索引中
fn new_log_file(path: &Path, gen: u64, readers: &mut HashMap<u64, BufReaderWithPos<File>>) -> Result<BufWriterWithPos<File>> {
    let path = load_path(path, gen);
    let writer = BufWriterWithPos::new(OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&path)?)?;
    //创建阅读器并加入索引
    readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}