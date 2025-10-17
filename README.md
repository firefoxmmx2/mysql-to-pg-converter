# MySQL to PostgreSQL 转换器

高性能的MySQL到PostgreSQL转换工具,采用分工架构,性能优异。

## 核心特性

- ✅ **双脚本架构**: DDL和INSERT分别处理,各司其职
- ✅ **高效INSERT转换**: 使用正则表达式,比sqlparse快10倍以上
- ✅ **数据类型转换**: 自动转换MySQL和PostgreSQL之间的数据类型差异
- ✅ **DDL语句转换**: 支持CREATE TABLE、索引、外键、注释等
- ✅ **大文件支持**: 流式处理,支持GB级文件
- ✅ **数据分割**: 自动分割大文件,支持并行导入
- ✅ **进度显示**: 实时显示处理进度

## 架构说明

本工具采用**分工明确**的双脚本架构:

1. **mysql_to_pg_converter.py** - 专注于表结构(DDL)转换
   - 使用sqlparse解析CREATE TABLE等复杂DDL语句
   - 处理数据类型映射、约束、索引、外键等
   
2. **extract_and_split_inserts.py** - 专注于INSERT语句转换
   - 使用高效的正则表达式直接处理INSERT
   - 支持大文件分割和并行导入
   - 性能比sqlparse快10倍以上

## 推荐工作流

```bash
# 步骤1: 转换表结构(快速)
python mysql_to_pg_converter.py input.sql schema.sql

# 步骤2: 提取并转换INSERT语句(高效)
python extract_and_split_inserts.py input.sql data_dir/ -s 200

# 步骤3: 导入表结构
psql -d mydb -U postgres -f schema.sql

# 步骤4: 并行导入数据(最快)
cd data_dir
python import_parallel.py mydb -u postgres -w 4
```

## 使用方法

### 方式1: 分步处理(推荐,最灵活)

```bash
# 只转换表结构
python mysql_to_pg_converter.py input.sql schema.sql

# 单独处理INSERT(可自定义分割大小)
python extract_and_split_inserts.py input.sql data/ -s 200
```

### 方式2: 一键处理

```bash
# 自动调用extract_and_split_inserts.py
python mysql_to_pg_converter.py input.sql schema.sql --process-inserts

# 自定义分割大小
python mysql_to_pg_converter.py input.sql schema.sql --process-inserts --chunk-size 100
```

### 输出文件

- `schema.sql` - 表结构文件(包含SEQUENCES、TABLES、INDEXES、FOREIGN KEYS、COMMENTS)
- `schema_data/` - 数据目录,包含:
  - `pg_data_part_001.sql`
  - `pg_data_part_002.sql`
  - ...
  - `import_parallel.py` - Python并行导入脚本(推荐)
  - `import_all.sh` - Linux/Mac批量导入脚本
  - `import_all.bat` - Windows批量导入脚本

### 导入到PostgreSQL

#### 并行导入(推荐,速度最快)

```bash
# 1. 导入表结构
psql -d your_database -f schema.sql

# 2. 并行导入数据(4个线程)
cd schema_data
python import_parallel.py your_database -u postgres -w 4
```

#### 顺序导入

```bash
# Linux/Mac
cd schema_data
./import_all.sh your_database postgres localhost 5432

# Windows
cd schema_data
import_all.bat your_database postgres localhost 5432
```

**并行导入优势**:
- 使用多线程同时导入多个文件
- 对于1.5GB的数据,4线程并行可将导入时间从30分钟降至约8分钟
- 自动处理失败重试

## 支持的转换

### 数据类型

| MySQL类型 | PostgreSQL类型 |
|-----------|---------------|
| tinyint | smallint |
| int | integer |
| bigint | bigint |
| float | real |
| double | double precision |
| decimal(m,n) | numeric(m,n) |
| varchar(n) | varchar(n) |
| text | text |
| datetime | timestamp |
| json | jsonb |
| enum(...) | varchar(255) + CHECK约束 |
| bit(1) | boolean |

### DDL特性

- ✅ AUTO_INCREMENT → SEQUENCE
- ✅ 主键约束
- ✅ 唯一键约束
- ✅ 外键约束(延迟到表创建后)
- ✅ 索引
- ✅ 列注释
- ✅ DEFAULT值转换

### DML特性

- ✅ INSERT语句转换(支持单行和多行)
  - 单行: `INSERT INTO table VALUES (val1, val2);`
  - 同行多VALUES: `INSERT INTO table VALUES (v1, v2), (v3, v4);`
  - 跨行: `INSERT INTO table VALUES (v1, v2), (v3, v4);` (跨多行)
- ✅ 反引号 → 双引号 (`table` → `"table"`)
- ✅ bit字面量转换 (b'0'/b'1' → '0'/'1')
- ✅ 转义字符处理 (\' → '')
- ✅ NULL值处理 (\N → NULL)

## 示例

### 输入 (MySQL)

```sql
CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  `status` enum('active','inactive') DEFAULT 'active',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `users` VALUES (1,'John','john@example.com','active','2024-01-01 10:00:00');
```

### 输出 (PostgreSQL)

**output_schema.sql:**
```sql
-- ===== SEQUENCES =====
CREATE SEQUENCE users_id_seq;

-- ===== TABLES =====
CREATE TABLE "users" (
    "id" integer DEFAULT nextval('users_id_seq') NOT NULL,
    "name" varchar(100) NOT NULL,
    "email" varchar(255) DEFAULT NULL,
    "status" varchar(255) DEFAULT 'active' CHECK ("status" IN ('active', 'inactive')),
    "created_at" timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ("id")
);

-- ===== INDEXES =====
CREATE INDEX "idx_email" ON "users" ("email");
```

**output_data.sql:**
```sql
-- ===== DATA =====
INSERT INTO "users" VALUES (1,'John','john@example.com','active','2024-01-01 10:00:00');
```

## 依赖

```bash
pip install sqlparse
```

## 注意事项

1. **外键约束**: 外键约束会被延迟到所有表创建完成后添加,避免循环依赖问题
2. **字符集**: 自动移除MySQL特有的CHARACTER SET和COLLATE子句
3. **引擎**: 自动移除MySQL的ENGINE子句
4. **大文件**: 建议对超过100MB的文件使用分离模式

## 完整工作流程示例

### 场景: 转换1.5GB的MySQL dump文件

```bash
# 1. 转换并分割数据(每个文件200MB)
python mysql_to_pg_converter.py large_dump.sql output.sql --split-data

# 输出:
# - output_schema.sql (表结构)
# - output_data/ (数据目录,包含8个分割文件)

# 2. 导入表结构
psql -d mydb -U postgres -f output_schema.sql

# 3. 并行导入数据(使用4个线程)
cd output_data
python import_parallel.py mydb -u postgres -w 4

# 完成! 总耗时约10-15分钟
```

### 性能对比

| 文件大小 | 模式 | 内存占用 | 转换时间 | 导入时间 |
|---------|------|---------|---------|---------|
| 1.5GB | 标准模式 | ~12GB | ~40分钟 | ~30分钟 |
| 1.5GB | 分离模式 | ~200MB | ~8分钟 | ~30分钟 |
| 1.5GB | 分割模式+并行导入(4线程) | ~200MB | ~10分钟 | ~8分钟 |

## 性能建议

- **小文件(<100MB)**: 使用标准模式
- **中等文件(100MB-1GB)**: 使用分离模式(`--separate`)
- **大文件(>1GB)**: 使用分割模式(`--split-data`)并配合并行导入
- 导入数据前可以先禁用索引和外键,导入后再启用(分割模式自动处理)
- 调整`--chunk-size`参数以平衡文件数量和并行效率
- 根据CPU核心数调整并行线程数(`-w`参数)

## 故障排除

### 问题: 找不到extract_and_split_inserts.py

**解决方案**: 确保 `extract_and_split_inserts.py` 和 `mysql_to_pg_converter.py` 在同一目录下。

### 问题: 内存占用过高

**解决方案**: 使用 `--split-data` 选项启用流式处理和数据分割。

### 问题: 导入速度慢

**解决方案**: 
1. 使用 `--split-data` 分割数据
2. 使用 `import_parallel.py` 并行导入
3. 增加并行线程数: `python import_parallel.py mydb -w 8`

## 依赖

```bash
pip install sqlparse
```

## License

MIT
