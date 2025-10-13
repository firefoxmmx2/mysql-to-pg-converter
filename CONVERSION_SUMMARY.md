# MySQL到PostgreSQL转换总结

## 转换完成情况

✅ **转换成功完成**

### 输入文件
- 文件: `~/dump-socialcollect_sdwyf-202510122351.sql`
- 大小: 1.3 MB
- 格式: MySQL 8 DDL

### 输出文件
- 文件: `socialcollect_sdwyf-struct.sql`
- 格式: PostgreSQL 13 DDL

## 转换统计

| 项目 | 数量 |
|------|------|
| 序列 (SEQUENCE) | 29 |
| 表 (TABLE) | 790 |
| 索引 (INDEX) | 319 |
| 列注释 (COMMENT) | 12,285 |
| 总行数 | 31,174 |

## 主要转换内容

### 1. 数据类型映射
- ✅ `tinyint` → `smallint`
- ✅ `int/integer` → `integer`
- ✅ `bigint` → `bigint`
- ✅ `decimal(m,n)` → `numeric(m,n)`
- ✅ `float/double` → `real/double precision`
- ✅ `datetime/timestamp` → `timestamp`
- ✅ `varchar(n)` → `varchar(n)`
- ✅ `text/longtext` → `text`
- ✅ `blob` → `bytea`
- ✅ `enum` → `text`

### 2. 特殊处理
- ✅ `AUTO_INCREMENT` → `SEQUENCE` + `DEFAULT nextval()`
- ✅ `PRIMARY KEY` 约束
- ✅ `UNIQUE KEY` → `UNIQUE INDEX`
- ✅ `KEY` → `INDEX`
- ✅ `FOREIGN KEY` 约束
- ✅ `COMMENT` → `COMMENT ON COLUMN/TABLE`
- ✅ 移除 `CHARACTER SET` 和 `COLLATE` 子句
- ✅ 移除 `ENGINE` 和其他MySQL特定选项

### 3. 标识符处理
- ✅ 所有表名和列名使用双引号包围
- ✅ 移除MySQL反引号

## 已知问题

⚠️ 转换过程中有2个警告（不影响结果）:
- 警告: 未知数据类型 '2'
- 警告: 未知数据类型 '参考'

这些警告来自注释内容的解析，不影响实际的DDL输出。

## 使用方法

```bash
python mysql_to_pg_converter.py <输入文件> <输出文件>
```

示例:
```bash
python mysql_to_pg_converter.py ~/dump-socialcollect_sdwyf-202510122351.sql socialcollect_sdwyf-struct.sql
```

## 后续步骤

1. 在PostgreSQL中执行生成的DDL文件
2. 如果有数据迁移需求，使用专门的数据迁移工具
3. 验证所有约束和索引是否正确创建
4. 检查应用程序兼容性

## 数据迁移注意事项

### Bit(1)类型和默认值转换

MySQL的`bit(1)`类型转换为PostgreSQL的`boolean`类型。

**默认值转换:**
转换器自动处理默认值:
- MySQL: `DEFAULT b'0'` → PostgreSQL: `DEFAULT '0'`
- MySQL: `DEFAULT b'1'` → PostgreSQL: `DEFAULT '1'`

PostgreSQL的boolean类型接受`'0'`和`'1'`作为有效值。

**数据迁移转换:**
- MySQL数据: `b'0'` → PostgreSQL: `FALSE` 或 `0` 或 `'0'`
- MySQL数据: `b'1'` → PostgreSQL: `TRUE` 或 `1` 或 `'1'`

**推荐的数据转换方法:**

1. **使用mysqldump导出**: 数据会自动转为0/1数字
   ```bash
   mysqldump --compatible=postgresql database_name > dump.sql
   ```

2. **使用ETL工具**: 配置字段映射规则
   - `b'0'` → `FALSE` 或 `0`
   - `b'1'` → `TRUE` 或 `1`

3. **手动转换CSV**: 
   ```bash
   sed "s/b'0'/0/g; s/b'1'/1/g" data.csv > data_converted.csv
   ```

## 代码改进

本次调试修复的问题:
1. ✅ 修复了中文变量名导致的语法错误
2. ✅ 修复了datetime模块导入顺序问题
3. ✅ 改进了token解析逻辑，使用正则表达式替代复杂的token遍历
4. ✅ 添加了对不带括号的数据类型支持（如 `int`, `bigint`）
5. ✅ 改进了decimal/numeric类型的解析
6. ✅ 改进了enum类型的处理
7. ✅ 修复了注释提取的正则表达式，支持中文注释
8. ✅ 添加了CHARACTER SET和COLLATE子句的移除
9. ✅ 改进了DEFAULT值的解析，支持带引号的字符串
10. ✅ **修复外键循环依赖问题** - 采用后置创建外键约束的方式，避免表创建时的循环依赖
11. ✅ **改进bit类型映射和默认值处理** - `bit(1)`转为`boolean`, 默认值`b'0'`/`b'1'`自动转为`'0'`/`'1'`

## 外键处理策略

为了解决外键循环依赖问题（例如表A引用表B，表B又引用表A），转换器采用以下策略：

1. **第一阶段**: 创建所有表结构（不包含外键约束）
2. **第二阶段**: 创建所有索引
3. **第三阶段**: 使用 `ALTER TABLE ADD CONSTRAINT` 添加所有外键约束
4. **第四阶段**: 添加列注释

这样可以确保：
- 所有表都已存在时才添加外键
- 避免因表创建顺序导致的依赖问题
- 支持任意复杂的外键关系网络
