# 坐标数据异步转换工具

将宽格式的用户坐标数据转换为长格式，并异步转换英国国家网格坐标系 (EPSG:27700) 为 WGS84 经纬度 (EPSG:4326)。

## 快速开始

```bash
# 1. 激活环境
uvact tc

# 2. 安装依赖
uv pip install -r requirements.txt

# 3. 配置 API KEY（.env 已配置）
# 支持单个 KEY
MAPTILER_API_KEY=your_key_here

# 或多个 KEY（用逗号分隔，自动切换）
MAPTILER_API_KEY=key1,key2,key3

# 4. 运行转换
cd src
python transform.py
```

## 重试失败的坐标

如果部分坐标转换失败，可以使用重试脚本：

```bash
# 1. 更新 .env 中的 API KEY（支持多个 KEY）
MAPTILER_API_KEY=new_key1,new_key2

# 2. 运行重试脚本
python retry_failed_coordinates.py
```

脚本会自动：
- 识别转换失败的坐标（longitude/latitude 为空）
- 使用新的 API KEY 批量重试
- 更新原文件中的数据
- 遇到 429 错误时自动切换到下一个 API KEY

## 快速测试

验证功能是否正常：

```bash
python quick_test.py
```

预期输出：
```
✅ 测试完成! 成功率: 10/10 (100.0%)
```

## 数据格式

### 输入（宽格式）
```csv
id,p32220_a0,p32223_a0,p32224_a0,p32220_a1,p32223_a1,p32224_a1,...
1,2007/12/18,430000,418000,,,,...
```
- `p32220_aX`: 时间 (YYYY/MM/DD)
- `p32223_aX`: X 坐标 (Easting)
- `p32224_aX`: Y 坐标 (Northing)

### 输出（长格式 + 经纬度）
```csv
p_id,a_id,year,month,day,x,y,longitude,latitude
1,a0,2007,12,18,430000,418000,-1.547538,53.657654
```

## API KEY 配置

### 单个 API KEY
```bash
# .env 文件
MAPTILER_API_KEY=your_key_here
```

### 多个 API KEY（推荐）
```bash
# .env 文件 - 用逗号分隔多个 KEY
MAPTILER_API_KEY=key1,key2,key3
```

**优势**：
- 🚀 自动切换：遇到 429 速率限制时自动切换到下一个 KEY
- 📊 统计追踪：记录每个 KEY 的使用情况
- ⚡ 提高效率：多个 KEY 可以突破单个 KEY 的速率限制

**工作原理**：
1. 程序使用第一个 API KEY 开始请求
2. 当遇到 429 错误时，自动切换到下一个 KEY
3. 继续请求直到所有坐标转换完成
4. 最后显示每个 KEY 的使用统计

## 性能

- **并发数**: 10（可配置）
- **预估速度**: 10-50 请求/秒（单 KEY）
- **33K 坐标**: 约 30-90 分钟（单 KEY）
- **多 KEY**: 速度可成倍提升

调整并发数：编辑 `src/transform.py` 中的 `MAX_CONCURRENT` 参数

## 故障排查

**依赖问题**
```bash
uvact tc
uv pip install aiohttp python-dotenv pandas
```

**API 错误**
- 检查 `.env` 中的 `MAPTILER_API_KEY`
- 免费配额：100K 请求/月

## 项目结构

```
transform_coordinates/
├── .env                           # API 密钥配置（支持多个 KEY）
├── README.md                      # 项目说明
├── requirements.txt               # 依赖列表
├── quick_test.py                  # 快速测试
├── retry_failed_coordinates.py    # 重试失败坐标的脚本
├── src/
│   └── transform.py               # 核心转换程序
└── data/
    ├── origin_data.csv            # 输入数据
    └── transformed_data.csv       # 输出数据
```

## 技术特点

- ✅ 异步 HTTP 请求 (aiohttp)
- ✅ 并发控制 (asyncio.Semaphore)
- ✅ 自动去重（避免重复转换）
- ✅ 多 API KEY 支持（自动切换）
- ✅ 429 速率限制自动处理
- ✅ API KEY 使用统计
- ✅ 进度显示和统计
- ✅ 完善的错误处理

## 许可证

MIT License
