# mysql_to_pg_converter.py

import sqlparse
import re
import argparse
import sys
import datetime
import os
import subprocess
from pathlib import Path

# --- 数据类型映射 ---
# 注意：顺序很重要，更具体的模式应该放在前面
TYPE_MAPPING = {
    # Numeric - 带括号的版本
    r'tinyint\(\d+\)': 'smallint',
    r'smallint\(\d+\)': 'smallint',
    r'mediumint\(\d+\)': 'integer',
    r'int\(\d+\)': 'integer',
    r'integer\(\d+\)': 'integer',
    r'bigint\(\d+\)': 'bigint',
    r'decimal\((\d+),(\d+)\)': r'numeric(\1,\2)',
    r'numeric\((\d+),(\d+)\)': r'numeric(\1,\2)',
    r'float\((\d+),(\d+)\)': r'real',
    r'bit\(1\)': 'boolean',  # bit(1) 转为 boolean
    r'bit\((\d+)\)': r'bit(\1)',  # bit(n) 保持为 bit(n)
    
    # Numeric - 不带括号的版本
    r'tinyint': 'smallint',
    r'smallint': 'smallint',
    r'mediumint': 'integer',
    r'int': 'integer',
    r'integer': 'integer',
    r'bigint': 'bigint',
    r'float': 'real',
    r'double': 'double precision',
    r'real': 'real',

    # Date and Time
    r'datetime\((\d+)\)': r'timestamp(\1)',
    r'datetime': 'timestamp',
    r'timestamp\((\d+)\)': r'timestamp(\1)',
    r'timestamp': 'timestamp',
    r'date': 'date',
    r'time\((\d+)\)': r'time(\1)',
    r'time': 'time',
    r'year\(\d+\)': 'smallint',
    r'year': 'smallint',

    # String
    r'char\((\d+)\)': r'char(\1)',
    r'varchar\((\d+)\)': r'varchar(\1)',
    r'binary\((\d+)\)': r'bytea',
    r'varbinary\((\d+)\)': r'bytea',
    r'tinyblob': 'bytea',
    r'blob': 'bytea',
    r'mediumblob': 'bytea',
    r'longblob': 'bytea',
    r'tinytext': 'text',
    r'text': 'text',
    r'mediumtext': 'text',
    r'longtext': 'text',
    r'json': 'jsonb',  # JSON类型转为JSONB
    r'enum\(.+\)': 'varchar(255)', # ENUM转换为varchar + CHECK约束
    r'set\(.+\)': 'text[]', # 将SET转换为文本数组
}

def convert_data_type(mysql_type_str):
    """转换单个数据类型字符串，返回(pg_type, enum_values)元组"""
    mysql_type_str = mysql_type_str.lower().strip()
    
    # 特殊处理 decimal 和 numeric 类型（保留精度）
    decimal_match = re.match(r'(decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', mysql_type_str, re.IGNORECASE)
    if decimal_match:
        return (f'numeric({decimal_match.group(2)},{decimal_match.group(3)})', None)
    
    # 特殊处理 float 和 double 带精度的情况
    float_match = re.match(r'(float|double)\s*\(\s*\d+\s*,\s*\d+\s*\)', mysql_type_str, re.IGNORECASE)
    if float_match:
        pg_type = 'real' if float_match.group(1).lower() == 'float' else 'double precision'
        return (pg_type, None)
    
    # 特殊处理 json 类型
    if mysql_type_str == 'json':
        return ('jsonb', None)
    
    # 特殊处理 enum 类型 - 提取枚举值
    enum_match = re.match(r"enum\((.+)\)", mysql_type_str, re.IGNORECASE)
    if enum_match:
        # 提取枚举值列表
        enum_values_str = enum_match.group(1)
        # 解析枚举值（处理单引号和双引号）
        enum_values = re.findall(r"'([^']*)'|\"([^\"]*)\"", enum_values_str)
        enum_values = [v[0] or v[1] for v in enum_values]
        return ('varchar(255)', enum_values)
    
    # 特殊处理 set 类型
    if mysql_type_str.startswith('set('):
        return ('text[]', None)
    
    for pattern, pg_type in TYPE_MAPPING.items():
        if re.fullmatch(pattern, mysql_type_str):
            return (re.sub(pattern, pg_type, mysql_type_str), None)
    # 如果没有匹配，返回原样并给出警告
    print(f"警告: 未知数据类型 '{mysql_type_str}'，将保持原样。", file=sys.stderr)
    return (mysql_type_str, None)

def quote_identifier(name):
    """为标识符加上双引号"""
    return f'"{name}"'

class DDLConverter:
    def __init__(self):
        self.sequences = set()
        self.indexes = set()
        self.comments = set()
        self.foreign_keys = []  # 存储外键约束,使用列表保持顺序

    def convert_create_table(self, statement):
        """转换单个 CREATE TABLE 语句"""
        pg_statements = []
        
        # 提取表名 - 使用更简单的方法
        stmt_str = str(statement)
        table_match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`"]?(\w+)[`"]?', stmt_str, re.IGNORECASE)
        if not table_match:
            return []
        table_name = quote_identifier(table_match.group(1))

        # 处理列定义
        column_definitions = []
        primary_key_def = None
        unique_constraints = []
        foreign_key_defs = []

        # 寻找括号内的内容
        parenthesis = None
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Parenthesis):
                parenthesis = token
                break
        
        if not parenthesis:
            return []
        
        # 获取括号内的所有 token
        tokens_in_parens = parenthesis.tokens[1:-1]  # 去掉括号本身

        # 使用正则表达式解析列定义（更简单可靠的方法）
        paren_content = str(parenthesis)[1:-1]  # 去掉括号
        
        # 分割列定义和约束（按逗号分割，但要注意括号内的逗号）
        items = self._split_by_comma(paren_content)
        
        for item in items:
            item = item.strip()
            if not item:
                continue
            
            # 处理主键
            if re.match(r'PRIMARY\s+KEY', item, re.IGNORECASE):
                pk_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', item, re.IGNORECASE)
                if pk_match:
                    pk_cols = [c.strip().strip('`"') for c in pk_match.group(1).split(',')]
                    primary_key_def = f"PRIMARY KEY ({', '.join([quote_identifier(c) for c in pk_cols])})"
            
            # 处理唯一键
            elif re.match(r'UNIQUE\s+KEY', item, re.IGNORECASE):
                unique_match = re.search(r'UNIQUE\s+KEY\s+[`"]?(\w+)[`"]?\s*\(([^)]+)\)', item, re.IGNORECASE)
                if unique_match:
                    index_name = quote_identifier(unique_match.group(1))
                    cols = [c.strip().strip('`"') for c in unique_match.group(2).split(',')]
                    self.indexes.add(f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({', '.join([quote_identifier(c) for c in cols])});")
            
            # 处理普通索引
            elif re.match(r'KEY\s+', item, re.IGNORECASE):
                key_match = re.search(r'KEY\s+[`"]?(\w+)[`"]?\s*\(([^)]+)\)', item, re.IGNORECASE)
                if key_match:
                    index_name = quote_identifier(key_match.group(1))
                    cols = [c.strip().strip('`"') for c in key_match.group(2).split(',')]
                    self.indexes.add(f"CREATE INDEX {index_name} ON {table_name} ({', '.join([quote_identifier(c) for c in cols])});")
            
            # 处理外键 - 收集但不添加到表定义中
            elif re.match(r'CONSTRAINT', item, re.IGNORECASE):
                fk_match = re.search(r'CONSTRAINT\s+[`"]?(\w+)[`"]?\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+[`"]?(\w+)[`"]?\s*\(([^)]+)\)', item, re.IGNORECASE)
                if fk_match:
                    fk_name = quote_identifier(fk_match.group(1))
                    fk_cols = [c.strip().strip('`"') for c in fk_match.group(2).split(',')]
                    ref_table = quote_identifier(fk_match.group(3))
                    ref_cols = [c.strip().strip('`"') for c in fk_match.group(4).split(',')]
                    # 生成ALTER TABLE语句,稍后添加
                    alter_stmt = f"ALTER TABLE {table_name} ADD CONSTRAINT {fk_name} FOREIGN KEY ({', '.join([quote_identifier(c) for c in fk_cols])}) REFERENCES {ref_table} ({', '.join([quote_identifier(c) for c in ref_cols])});"
                    self.foreign_keys.append(alter_stmt)
            
            # 处理列定义
            else:
                col_def = self._convert_column_definition(item, table_name)
                if col_def:
                    column_definitions.append(col_def)

        # 组装最终的 CREATE TABLE 语句 (不包含外键约束)
        all_defs = column_definitions
        if primary_key_def:
            all_defs.append(primary_key_def)
        # 不再添加 foreign_key_defs,它们已经被收集到 self.foreign_keys 中

        pg_statements.append(f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(all_defs) + "\n);")
        
        return pg_statements

    def _split_by_comma(self, text):
        """按逗号分割，但忽略括号内和引号内的逗号"""
        items = []
        current = []
        paren_depth = 0
        quote_char = None
        
        i = 0
        while i < len(text):
            char = text[i]
            
            # 处理引号
            if char in ("'", '"'):
                if quote_char is None:
                    quote_char = char
                elif quote_char == char:
                    # 检查是否是转义的引号
                    if i + 1 < len(text) and text[i + 1] == char:
                        current.append(char)
                        current.append(char)
                        i += 2
                        continue
                    else:
                        quote_char = None
                current.append(char)
            # 只在引号外处理括号和逗号
            elif quote_char is None:
                if char == '(':
                    paren_depth += 1
                    current.append(char)
                elif char == ')':
                    paren_depth -= 1
                    current.append(char)
                elif char == ',' and paren_depth == 0:
                    items.append(''.join(current))
                    current = []
                    i += 1
                    continue
                else:
                    current.append(char)
            else:
                current.append(char)
            
            i += 1
        
        if current:
            items.append(''.join(current))
        
        return items
    
    def _convert_column_definition(self, col_def_str, table_name):
        """转换单个列定义"""
        # 解析列定义：列名 类型 [约束...]
        col_def_str = col_def_str.strip()
        
        # 提取列名（可能被反引号包围）
        col_match = re.match(r'[`"]?(\w+)[`"]?\s+(.+)', col_def_str, re.IGNORECASE)
        if not col_match:
            return None
        
        col_name = quote_identifier(col_match.group(1))
        rest = col_match.group(2)
        
        # 移除 CHARACTER SET 和 COLLATE 子句
        rest = re.sub(r'\s+CHARACTER\s+SET\s+\w+', '', rest, flags=re.IGNORECASE)
        rest = re.sub(r'\s+COLLATE\s+\w+', '', rest, flags=re.IGNORECASE)
        
        # 提取数据类型（包括括号内的参数，如 decimal(10,2)）
        # 使用非贪婪匹配，并在遇到关键字（NOT NULL, DEFAULT, COMMENT等）前停止
        type_match = re.match(r'([\w]+(?:\([^)]+\))?(?:\s+UNSIGNED)?)(?:\s+(?:NOT\s+NULL|NULL|DEFAULT|COMMENT|AUTO_INCREMENT|PRIMARY|UNIQUE|KEY)|\s*$|,)', rest, re.IGNORECASE)
        if not type_match:
            return None
        
        mysql_type = type_match.group(1).replace(' UNSIGNED', '').strip()
        pg_type, enum_values = convert_data_type(mysql_type)
        
        col_def = f"{col_name} {pg_type}"
        
        # 处理 NOT NULL
        if re.search(r'\bNOT\s+NULL\b', rest, re.IGNORECASE):
            col_def += " NOT NULL"
        
        # 处理 DEFAULT（需要处理带引号的字符串值）
        default_match = re.search(r"DEFAULT\s+('(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[^\s,]+)", rest, re.IGNORECASE)
        if default_match:
            default_val = default_match.group(1).strip()
            # 处理特殊的MySQL默认值
            if default_val.upper() == 'CURRENT_TIMESTAMP':
                default_val = 'CURRENT_TIMESTAMP'
            elif default_val.upper() == 'NULL':
                default_val = 'NULL'
            # 处理 bit 类型的默认值 b'0' 和 b'1'
            elif default_val == "b'0'" or default_val == 'b"0"':
                default_val = "'0'"
            elif default_val == "b'1'" or default_val == 'b"1"':
                default_val = "'1'"
            col_def += f" DEFAULT {default_val}"
        
        # 处理 AUTO_INCREMENT
        if re.search(r'\bAUTO_INCREMENT\b', rest, re.IGNORECASE):
            seq_name = f"{table_name.strip('\"')}_{col_name.strip('\"')}_seq"
            self.sequences.add(f"CREATE SEQUENCE {seq_name};")
            col_def = f"{col_name} {pg_type} DEFAULT nextval('{seq_name}')"
            if 'NOT NULL' not in col_def:
                col_def += " NOT NULL"
        
        # 如果是ENUM类型，添加CHECK约束
        if enum_values:
            # 转义单引号并构建CHECK约束
            escaped_values = [v.replace("'", "''") for v in enum_values]
            check_values = "', '".join(escaped_values)
            col_def += f" CHECK ({col_name} IN ('{check_values}'))"
        
        # 处理列注释（支持中文和各种引号）
        comment_match = re.search(r"COMMENT\s+(['\"])(.+?)\1", rest, re.IGNORECASE)
        if comment_match:
            comment_val = comment_match.group(2).replace("'", "''")  # 转义单引号
            self.comments.add(f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment_val}';")
        
        return col_def

    def convert_file(self, input_path, output_path, process_inserts=False, chunk_size_mb=200):
        """主函数，读取文件并转换表结构
        
        Args:
            input_path: 输入的MySQL SQL文件路径
            output_path: 输出的PostgreSQL SQL文件路径（表结构）
            process_inserts: 是否同时处理INSERT语句（使用extract_and_split_inserts.py）
            chunk_size_mb: INSERT分割时每个文件的大小(MB)
        """
        file_size = os.path.getsize(input_path)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"处理文件: {input_path} ({file_size_mb:.1f} MB)")
        print(f"只转换表结构(DDL)到: {output_path}")
        
        # 处理表结构
        self._process_schema(input_path, output_path)
        
        # 如果需要处理INSERT语句,调用extract_and_split_inserts.py
        if process_inserts:
            self._process_inserts_with_extractor(input_path, output_path, chunk_size_mb)
    
    def _process_schema(self, input_path, output_path):
        """处理表结构(DDL语句)"""
        print(f"开始处理表结构...")
        pg_output = []
        
        with open(input_path, 'r', encoding='utf-8') as f:
            buffer = []
            in_statement = False
            statement_type = None
            line_num = 0
            
            for line in f:
                line_num += 1
                
                # 跳过SET语句
                if re.match(r'\s*SET\s+', line, re.IGNORECASE):
                    continue
                
                # 检测语句开始
                if not in_statement:
                    if re.match(r'\s*CREATE\s+TABLE', line, re.IGNORECASE):
                        in_statement = True
                        statement_type = 'CREATE'
                        buffer = [line]
                    continue
                
                # 收集语句内容
                buffer.append(line)
                
                # 检测语句结束(以分号结尾)
                if line.strip().endswith(';'):
                    statement_str = ''.join(buffer)
                    
                    if statement_type == 'CREATE':
                        # 解析并转换CREATE TABLE语句
                        try:
                            parsed = sqlparse.parse(statement_str)
                            if parsed:
                                converted = self.convert_create_table(parsed[0])
                                pg_output.extend(converted)
                        except Exception as e:
                            print(f"警告: 第{line_num}行附近的语句解析失败: {e}", file=sys.stderr)
                    
                    # 重置状态
                    buffer = []
                    in_statement = False
                    statement_type = None
                
                # 进度显示(每10000行)
                if line_num % 10000 == 0:
                    print(f"  已处理 {line_num} 行...")
        
        # 写入结构文件
        print(f"写入表结构到: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("-- ===== PostgreSQL 表结构 =====\n")
            f.write("-- 从MySQL转换而来\n")
            f.write(f"-- 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-- \n")
            f.write("-- 注意: 此文件只包含表结构(DDL),不包含数据(INSERT)\n")
            f.write("-- 数据请使用 extract_and_split_inserts.py 单独处理\n\n")
            
            f.write("-- ===== SEQUENCES =====\n")
            f.write("\n".join(sorted(list(self.sequences))))
            f.write("\n\n-- ===== TABLES =====\n")
            f.write("\n".join(pg_output))
            f.write("\n\n-- ===== INDEXES =====\n")
            f.write("\n".join(sorted(list(self.indexes))))
            f.write("\n\n-- ===== FOREIGN KEYS =====\n")
            f.write("-- 外键约束在所有表创建完成后添加,避免循环依赖问题\n")
            f.write("\n".join(self.foreign_keys))
            f.write("\n\n-- ===== COMMENTS =====\n")
            f.write("\n".join(sorted(list(self.comments))))
            f.write("\n")
        
        print("表结构转换完成！")
    
    def _process_inserts_with_extractor(self, input_path, output_path, chunk_size_mb):
        """使用extract_and_split_inserts.py处理INSERT语句"""
        # 查找extract_and_split_inserts.py脚本
        script_dir = Path(__file__).parent
        split_script = script_dir / "extract_and_split_inserts.py"
        
        if not split_script.exists():
            print(f"\n警告: 未找到 extract_and_split_inserts.py 脚本")
            print(f"      期望位置: {split_script}")
            print(f"      跳过INSERT语句处理")
            return
        
        # 生成输出目录
        base_name = output_path.rsplit('.', 1)[0]
        output_dir = f"{base_name}_data"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 构建命令
        cmd = [
            sys.executable,
            str(split_script),
            input_path,
            output_dir,
            '-s', str(chunk_size_mb),
            '-p', 'pg_data'
        ]
        
        print(f"\n处理INSERT语句...")
        print(f"  使用脚本: extract_and_split_inserts.py")
        print(f"  输出目录: {output_dir}")
        print(f"  分割大小: {chunk_size_mb}MB")
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=False, text=True)
            print(f"\nINSERT语句处理完成!")
            print(f"  数据文件: {output_dir}/")
        except subprocess.CalledProcessError as e:
            print(f"\n错误: INSERT语句处理失败")
            print(f"  命令: {' '.join(cmd)}")
            print(f"  返回码: {e.returncode}")
            raise
        except FileNotFoundError:
            print(f"\n错误: 无法执行Python解释器: {sys.executable}")
            raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="MySQL DDL to PostgreSQL DDL Converter (只转换表结构)",
        epilog="""
注意事项:
  - 此脚本只转换表结构(DDL),不处理INSERT语句
  - INSERT语句请使用 extract_and_split_inserts.py 单独处理(效率更高)
  - extract_and_split_inserts.py 支持分割大文件和并行导入

示例:
  # 只转换表结构
  python mysql_to_pg_converter.py input.sql schema.sql
  
  # 转换表结构并同时处理INSERT语句(自动调用extract_and_split_inserts.py)
  python mysql_to_pg_converter.py input.sql schema.sql --process-inserts
  
  # 自定义INSERT分割大小(每个文件100MB)
  python mysql_to_pg_converter.py input.sql schema.sql --process-inserts --chunk-size 100

推荐工作流:
  1. 先转换表结构: python mysql_to_pg_converter.py input.sql schema.sql
  2. 再处理INSERT: python extract_and_split_inserts.py input.sql data_dir/ -s 200
  3. 导入表结构: psql -d dbname -f schema.sql
  4. 并行导入数据: python data_dir/import_parallel.py dbname -w 4
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("input_file", help="输入的MySQL .sql文件路径")
    parser.add_argument("output_file", help="输出的PostgreSQL表结构文件路径")
    parser.add_argument(
        "--process-inserts",
        action="store_true",
        help="同时处理INSERT语句(自动调用extract_and_split_inserts.py)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=200,
        help="INSERT分割时每个文件的大小(MB),默认200MB"
    )
    
    args = parser.parse_args()
    
    converter = DDLConverter()
    converter.convert_file(
        args.input_file, 
        args.output_file,
        process_inserts=args.process_inserts,
        chunk_size_mb=args.chunk_size
    )

