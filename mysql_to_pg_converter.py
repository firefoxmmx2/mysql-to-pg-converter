# mysql_to_pg_converter.py

import sqlparse
import re
import argparse
import sys
import datetime

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
    r'datetime\(\d+\)': 'timestamp(6)',
    r'datetime': 'timestamp',
    r'timestamp\(\d+\)': 'timestamp(6)',
    r'timestamp': 'timestamp',
    r'date': 'date',
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
    r'enum\(.+\)': 'text', # 简化处理，PostgreSQL有原生ENUM类型，但需要先创建
    r'set\(.+\)': 'text[]', # 将SET转换为文本数组
}

def convert_data_type(mysql_type_str):
    """转换单个数据类型字符串"""
    mysql_type_str = mysql_type_str.lower().strip()
    
    # 特殊处理 decimal 和 numeric 类型（保留精度）
    decimal_match = re.match(r'(decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', mysql_type_str, re.IGNORECASE)
    if decimal_match:
        return f'numeric({decimal_match.group(2)},{decimal_match.group(3)})'
    
    # 特殊处理 float 和 double 带精度的情况
    float_match = re.match(r'(float|double)\s*\(\s*\d+\s*,\s*\d+\s*\)', mysql_type_str, re.IGNORECASE)
    if float_match:
        return 'real' if float_match.group(1).lower() == 'float' else 'double precision'
    
    # 特殊处理 enum 类型
    if mysql_type_str.startswith('enum('):
        return 'text'
    
    # 特殊处理 set 类型
    if mysql_type_str.startswith('set('):
        return 'text[]'
    
    for pattern, pg_type in TYPE_MAPPING.items():
        if re.fullmatch(pattern, mysql_type_str):
            return re.sub(pattern, pg_type, mysql_type_str)
    # 如果没有匹配，返回原样并给出警告
    print(f"警告: 未知数据类型 '{mysql_type_str}'，将保持原样。", file=sys.stderr)
    return mysql_type_str

def quote_identifier(name):
    """为标识符加上双引号"""
    return f'"{name}"'

class DDLConverter:
    def __init__(self):
        self.sequences = set()
        self.indexes = set()
        self.comments = set()
        self.foreign_keys = []  # 存储外键约束,使用列表保持顺序
        self.data_statements = []

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
        """按逗号分割，但忽略括号内的逗号"""
        items = []
        current = []
        paren_depth = 0
        
        for char in text:
            if char == '(':
                paren_depth += 1
                current.append(char)
            elif char == ')':
                paren_depth -= 1
                current.append(char)
            elif char == ',' and paren_depth == 0:
                items.append(''.join(current))
                current = []
            else:
                current.append(char)
        
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
        type_match = re.match(r'([\w]+(?:\([^)]+\))?(?:\s+UNSIGNED)?)', rest, re.IGNORECASE)
        if not type_match:
            return None
        
        mysql_type = type_match.group(1).replace(' UNSIGNED', '').strip()
        pg_type = convert_data_type(mysql_type)
        
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
            seq_name = f"{table_name.strip('"')}_{col_name.strip('"')}_seq"
            self.sequences.add(f"CREATE SEQUENCE {seq_name};")
            col_def = f"{col_name} {pg_type} DEFAULT nextval('{seq_name}')"
            if 'NOT NULL' not in col_def:
                col_def += " NOT NULL"
        
        # 处理列注释（支持中文和各种引号）
        comment_match = re.search(r"COMMENT\s+(['\"])(.+?)\1", rest, re.IGNORECASE)
        if comment_match:
            comment_val = comment_match.group(2).replace("'", "''")  # 转义单引号
            self.comments.add(f"COMMENT ON COLUMN {table_name}.{col_name} IS '{comment_val}';")
        
        return col_def

    def convert_file(self, input_path, output_path):
        """主函数，读取文件并转换"""
        print(f"开始读取文件: {input_path}")
        with open(input_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # 移除 SET 语句等
        sql_content = re.sub(r"SET\s+@\w+.*?;", "", sql_content, flags=re.IGNORECASE | re.DOTALL)
        sql_content = re.sub(r"SET\s+NAMES.*?;", "", sql_content, flags=re.IGNORECASE)
        
        parsed_statements = sqlparse.parse(sql_content)

        pg_output = []
        self.data_statements = []

        for stmt in parsed_statements:
            if stmt.get_type() == 'CREATE':
                # 使用字符串匹配检查是否是 CREATE TABLE
                stmt_str = str(stmt).strip()
                if re.match(r'CREATE\s+TABLE', stmt_str, re.IGNORECASE):
                    converted = self.convert_create_table(stmt)
                    pg_output.extend(converted)
            elif stmt.get_type() == 'INSERT':
                converted_insert = self.convert_insert_statement(stmt)
                if converted_insert:
                    self.data_statements.append(converted_insert)

        # 将所有收集到的语句按正确顺序组合
        final_output = []
        final_output.append("-- ===== SEQUENCES =====")
        final_output.extend(sorted(list(self.sequences)))
        final_output.append("\n-- ===== TABLES =====")
        final_output.extend(pg_output)
        if self.data_statements:
            final_output.append("\n-- ===== DATA =====")
            final_output.extend(self.data_statements)
        final_output.append("\n-- ===== INDEXES =====")
        final_output.extend(sorted(list(self.indexes)))
        final_output.append("\n-- ===== FOREIGN KEYS =====")
        final_output.append("-- 外键约束在所有表创建完成后添加,避免循环依赖问题")
        final_output.extend(self.foreign_keys)
        final_output.append("\n-- ===== COMMENTS =====")
        final_output.extend(sorted(list(self.comments)))

        print(f"转换完成，正在写入文件: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(final_output))
        
        print("转换成功！")

    def convert_insert_statement(self, statement):
        """转换单个 INSERT 语句"""
        stmt_str = str(statement).strip()
        if not stmt_str:
            return None

        # 将反引号标识符替换为双引号
        def replace_identifier(match):
            identifier = match.group(1)
            return f'"{identifier}"'

        converted = re.sub(r'`([^`]+)`', replace_identifier, stmt_str)

        # 处理 bit 字面量 b'0'/b'1'
        converted = re.sub(r"\bb'([01])'", r"'\1'", converted)
        converted = re.sub(r'\bb"([01])"', r"'\1'", converted)

        # MySQL 中的转义单引号 \'
        converted = converted.replace("\\'", "''")

        # MySQL 的 \N 表示 NULL
        converted = re.sub(r'(?<!\\)\\N', 'NULL', converted)

        return converted


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="MySQL DDL to PostgreSQL DDL Converter")
    parser.add_argument("input_file", help="Path to the input MySQL .sql file")
    parser.add_argument("output_file", help="Path for the output PostgreSQL .sql file")
    
    args = parser.parse_args()
    
    converter = DDLConverter()
    converter.convert_file(args.input_file, args.output_file)

