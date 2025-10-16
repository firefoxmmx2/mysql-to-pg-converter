#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从MySQL dump文件中提取INSERT语句，转换为PostgreSQL格式，并分割成多个文件
专门处理超大文件（1.4GB+），支持Windows换行符
"""

import re
import os
import sys
import argparse
from pathlib import Path


class InsertExtractorAndSplitter:
    """提取INSERT语句并转换为PostgreSQL格式，然后分割文件"""
    
    def __init__(self, chunk_size_mb=200):
        """
        初始化
        :param chunk_size_mb: 每个分割文件的大小（MB）
        """
        self.chunk_size_bytes = chunk_size_mb * 1024 * 1024
        self.current_chunk_size = 0
        self.chunk_number = 1
        self.total_inserts = 0
        
    def convert_insert_line(self, line):
        """
        转换单行INSERT语句从MySQL格式到PostgreSQL格式
        :param line: 原始INSERT语句行
        :return: 转换后的语句，如果不是INSERT语句则返回None
        """
        # 去除行尾的Windows和Unix换行符
        line = line.rstrip('\r\n')
        
        # 检查是否是INSERT语句
        if not re.match(r'^\s*INSERT\s+INTO', line, re.IGNORECASE):
            return None
        
        # 1. 将反引号标识符替换为双引号（MySQL使用`table`，PostgreSQL使用"table"）
        converted = re.sub(r'`([^`]+)`', r'"\1"', line)
        
        # 2. 处理bit字面量 b'0'/b'1' -> '0'/'1'
        converted = re.sub(r"\bb'([01])'", r"'\1'", converted)
        converted = re.sub(r'\bb"([01])"', r"'\1'", converted)
        
        # 3. MySQL中的转义单引号 \' -> ''（PostgreSQL标准）
        converted = converted.replace("\\'", "''")
        
        # 4. MySQL的 \N 表示 NULL
        converted = re.sub(r'(?<!\\)\\N', 'NULL', converted)
        
        # 5. 处理其他常见的MySQL转义序列
        # \0 -> 空字符（在PostgreSQL中需要特殊处理，这里简化为空）
        converted = converted.replace('\\0', '')
        
        # 6. 处理双反斜杠 \\ -> \
        converted = converted.replace('\\\\', '\\')
        
        # 确保语句以分号结尾
        if not converted.rstrip().endswith(';'):
            converted += ';'
        
        return converted
    
    def process_file(self, input_file, output_dir, output_prefix='pg_inserts'):
        """
        处理输入文件，提取INSERT语句并分割
        :param input_file: 输入的MySQL dump文件路径
        :param output_dir: 输出目录
        :param output_prefix: 输出文件前缀
        """
        # 确保输出目录存在
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # 获取输入文件大小
        input_size = os.path.getsize(input_file)
        print(f"输入文件大小: {input_size / (1024**3):.2f} GB")
        print(f"开始处理文件: {input_file}")
        print(f"输出目录: {output_dir}")
        print(f"每个分割文件大小: {self.chunk_size_bytes / (1024**2):.0f} MB")
        print("-" * 60)
        
        # 打开第一个输出文件
        current_output_file = output_path / f"{output_prefix}_part_{self.chunk_number:03d}.sql"
        output_handle = open(current_output_file, 'w', encoding='utf-8', newline='\n')
        
        # 写入文件头注释
        self._write_header(output_handle, self.chunk_number)
        
        processed_bytes = 0
        line_number = 0
        
        try:
            # 使用二进制模式读取，然后解码，以便正确处理Windows换行符
            with open(input_file, 'rb') as f:
                for binary_line in f:
                    line_number += 1
                    processed_bytes += len(binary_line)
                    
                    # 解码行（处理可能的编码问题）
                    try:
                        line = binary_line.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            line = binary_line.decode('latin-1')
                        except:
                            print(f"警告: 第 {line_number} 行解码失败，跳过", file=sys.stderr)
                            continue
                    
                    # 转换INSERT语句
                    converted_line = self.convert_insert_line(line)
                    
                    if converted_line:
                        # 检查是否需要切换到新文件
                        line_bytes = len(converted_line.encode('utf-8')) + 1  # +1 for newline
                        
                        if self.current_chunk_size + line_bytes > self.chunk_size_bytes and self.current_chunk_size > 0:
                            # 写入文件尾部并关闭当前文件
                            self._write_footer(output_handle)
                            output_handle.close()
                            print(f"完成文件 {self.chunk_number}: {current_output_file.name} "
                                  f"({self.current_chunk_size / (1024**2):.2f} MB, "
                                  f"{self.total_inserts} 条INSERT语句)")
                            
                            # 打开新文件
                            self.chunk_number += 1
                            self.current_chunk_size = 0
                            current_output_file = output_path / f"{output_prefix}_part_{self.chunk_number:03d}.sql"
                            output_handle = open(current_output_file, 'w', encoding='utf-8', newline='\n')
                            self._write_header(output_handle, self.chunk_number)
                        
                        # 写入转换后的语句
                        output_handle.write(converted_line + '\n')
                        self.current_chunk_size += line_bytes
                        self.total_inserts += 1
                    
                    # 显示进度
                    if line_number % 10000 == 0:
                        progress = (processed_bytes / input_size) * 100
                        print(f"处理进度: {progress:.1f}% "
                              f"(第 {line_number:,} 行, "
                              f"已提取 {self.total_inserts:,} 条INSERT语句, "
                              f"当前文件: part_{self.chunk_number:03d})", 
                              end='\r')
        
        finally:
            # 写入文件尾部并关闭最后一个文件
            if output_handle and not output_handle.closed:
                self._write_footer(output_handle)
                output_handle.close()
                print(f"\n完成文件 {self.chunk_number}: {current_output_file.name} "
                      f"({self.current_chunk_size / (1024**2):.2f} MB, "
                      f"{self.total_inserts} 条INSERT语句)")
        
        print("-" * 60)
        print(f"处理完成！")
        print(f"总共提取 {self.total_inserts:,} 条INSERT语句")
        print(f"生成 {self.chunk_number} 个文件")
        print(f"输出目录: {output_dir}")
        
        # 生成导入脚本
        self._generate_import_script(output_path, output_prefix, self.chunk_number)
    
    def _write_header(self, file_handle, chunk_num):
        """写入文件头注释和性能优化设置"""
        file_handle.write(f"-- PostgreSQL INSERT语句 - Part {chunk_num}\n")
        file_handle.write(f"-- 从MySQL dump转换而来\n")
        file_handle.write(f"-- 生成时间: {self._get_timestamp()}\n")
        file_handle.write(f"--\n")
        file_handle.write(f"-- 注意: 请确保表结构已经创建\n")
        file_handle.write(f"--\n\n")
        
        # 设置错误处理 - 遇到错误时继续执行（必须在最前面）
        file_handle.write(f"-- 错误处理：遇到错误时继续执行后续语句\n")
        file_handle.write(f"\\set ON_ERROR_STOP off\n\n")
        
        # 设置会话复制模式，禁用触发器和外键约束检查（提升导入性能）
        file_handle.write(f"-- 设置会话为复制模式（禁用触发器和约束检查）\n")
        file_handle.write(f"SET session_replication_role = 'replica';\n\n")
        
        # 其他性能优化设置
        file_handle.write(f"-- 性能优化设置\n")
        file_handle.write(f"SET synchronous_commit = OFF;\n")
        file_handle.write(f"SET maintenance_work_mem = '256MB';\n\n")
    
    def _write_footer(self, file_handle):
        """写入文件尾部，恢复设置"""
        file_handle.write(f"\n-- 恢复会话设置\n")
        file_handle.write(f"SET session_replication_role = 'origin';\n")
    
    def _get_timestamp(self):
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def _generate_import_script(self, output_path, prefix, total_chunks):
        """生成批量导入脚本"""
        # 生成Windows批处理脚本
        bat_script = output_path / f"import_all.bat"
        with open(bat_script, 'w', encoding='utf-8') as f:
            f.write("@echo off\n")
            f.write("REM PostgreSQL批量导入脚本 - Windows版本\n")
            f.write("REM 使用方法: import_all.bat <database_name> <username> <host> <port>\n")
            f.write("REM 示例: import_all.bat mydb postgres localhost 5432\n\n")
            f.write("setlocal\n\n")
            f.write("set DB_NAME=%1\n")
            f.write("set DB_USER=%2\n")
            f.write("set DB_HOST=%3\n")
            f.write("set DB_PORT=%4\n\n")
            f.write("if \"%DB_NAME%\"==\"\" (\n")
            f.write("    echo 错误: 请提供数据库名称\n")
            f.write("    echo 使用方法: import_all.bat ^<database_name^> ^<username^> ^<host^> ^<port^>\n")
            f.write("    exit /b 1\n")
            f.write(")\n\n")
            f.write("if \"%DB_USER%\"==\"\" set DB_USER=postgres\n")
            f.write("if \"%DB_HOST%\"==\"\" set DB_HOST=localhost\n")
            f.write("if \"%DB_PORT%\"==\"\" set DB_PORT=5432\n\n")
            f.write("REM 如果环境变量中没有设置密码,则提示输入一次\n")
            f.write("if \"%PGPASSWORD%\"==\" \" (\n")
            f.write("    set /p PGPASSWORD=请输入数据库密码(输入后将用于所有导入操作): \n")
            f.write(")\n\n")
            f.write("echo 开始导入数据到数据库: %DB_NAME%\n")
            f.write("echo 用户: %DB_USER%\n")
            f.write("echo 主机: %DB_HOST%:%DB_PORT%\n")
            f.write("echo.\n\n")
            
            for i in range(1, total_chunks + 1):
                filename = f"{prefix}_part_{i:03d}.sql"
                f.write(f"echo 导入文件 {i}/{total_chunks}: {filename}\n")
                f.write(f"psql -h %DB_HOST% -p %DB_PORT% -U %DB_USER% -d %DB_NAME% -f {filename}\n")
                f.write(f"if errorlevel 1 (\n")
                f.write(f"    echo 错误: 导入 {filename} 失败\n")
                f.write(f"    exit /b 1\n")
                f.write(f")\n")
                f.write(f"echo.\n\n")
            
            f.write("echo 所有文件导入完成！\n")
            f.write("endlocal\n")
        
        # 生成Linux/Mac bash脚本
        sh_script = output_path / f"import_all.sh"
        with open(sh_script, 'w', encoding='utf-8', newline='\n') as f:
            f.write("#!/bin/bash\n")
            f.write("# PostgreSQL批量导入脚本 - Linux/Mac版本\n")
            f.write("# 使用方法: ./import_all.sh <database_name> <username> <host> <port>\n")
            f.write("# 示例: ./import_all.sh mydb postgres localhost 5432\n\n")
            f.write("DB_NAME=$1\n")
            f.write("DB_USER=${2:-postgres}\n")
            f.write("DB_HOST=${3:-localhost}\n")
            f.write("DB_PORT=${4:-5432}\n\n")
            f.write("if [ -z \"$DB_NAME\" ]; then\n")
            f.write("    echo \"错误: 请提供数据库名称\"\n")
            f.write("    echo \"使用方法: ./import_all.sh <database_name> <username> <host> <port>\"\n")
            f.write("    exit 1\n")
            f.write("fi\n\n")
            f.write("# 如果环境变量中没有设置密码,则提示输入一次\n")
            f.write("if [ -z \"$PGPASSWORD\" ]; then\n")
            f.write("    echo \"请输入数据库密码(输入后将用于所有导入操作):\"\n")
            f.write("    read -s PGPASSWORD\n")
            f.write("    export PGPASSWORD\n")
            f.write("    echo\n")
            f.write("fi\n\n")
            f.write("echo \"开始导入数据到数据库: $DB_NAME\"\n")
            f.write("echo \"用户: $DB_USER\"\n")
            f.write("echo \"主机: $DB_HOST:$DB_PORT\"\n")
            f.write("echo\n\n")
            
            for i in range(1, total_chunks + 1):
                filename = f"{prefix}_part_{i:03d}.sql"
                f.write(f"echo \"导入文件 {i}/{total_chunks}: {filename}\"\n")
                f.write(f"psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f {filename}\n")
                f.write(f"if [ $? -ne 0 ]; then\n")
                f.write(f"    echo \"错误: 导入 {filename} 失败\"\n")
                f.write(f"    exit 1\n")
                f.write(f"fi\n")
                f.write(f"echo\n\n")
            
            f.write("echo \"所有文件导入完成！\"\n")
        
        # 设置bash脚本为可执行
        try:
            os.chmod(sh_script, 0o755)
        except:
            pass
        
        # 生成Python并行导入脚本
        py_script = output_path / f"import_parallel.py"
        with open(py_script, 'w', encoding='utf-8') as f:
            f.write("#!/usr/bin/env python3\n")
            f.write("# -*- coding: utf-8 -*-\n")
            f.write('"""\n')
            f.write("PostgreSQL并行导入脚本\n")
            f.write("使用多线程同时导入多个文件以加快速度\n")
            f.write('"""\n\n')
            f.write("import subprocess\n")
            f.write("import sys\n")
            f.write("import argparse\n")
            f.write("import os\n")
            f.write("import getpass\n")
            f.write("from concurrent.futures import ThreadPoolExecutor, as_completed\n")
            f.write("from pathlib import Path\n\n\n")
            f.write("def import_file(file_path, db_name, db_user, db_host, db_port, env):\n")
            f.write('    """导入单个SQL文件"""\n')
            f.write("    cmd = [\n")
            f.write("        'psql',\n")
            f.write("        '-h', db_host,\n")
            f.write("        '-p', str(db_port),\n")
            f.write("        '-U', db_user,\n")
            f.write("        '-d', db_name,\n")
            f.write("        '-f', str(file_path)\n")
            f.write("    ]\n")
            f.write("    \n")
            f.write("    try:\n")
            f.write("        result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)\n")
            f.write("        return (file_path.name, True, None)\n")
            f.write("    except subprocess.CalledProcessError as e:\n")
            f.write("        return (file_path.name, False, e.stderr)\n\n\n")
            f.write("def main():\n")
            f.write("    parser = argparse.ArgumentParser(description='并行导入PostgreSQL数据')\n")
            f.write("    parser.add_argument('database', help='数据库名称')\n")
            f.write("    parser.add_argument('-u', '--user', default='postgres', help='数据库用户名')\n")
            f.write("    parser.add_argument('-H', '--host', default='localhost', help='数据库主机')\n")
            f.write("    parser.add_argument('-p', '--port', default=5432, type=int, help='数据库端口')\n")
            f.write(f"    parser.add_argument('-w', '--workers', default=4, type=int, help='并行线程数（默认4）')\n")
            f.write(f"    parser.add_argument('--prefix', default='{prefix}', help='文件前缀')\n")
            f.write("    parser.add_argument('--password', help='数据库密码(不推荐,建议使用环境变量PGPASSWORD)')\n")
            f.write("    \n")
            f.write("    args = parser.parse_args()\n")
            f.write("    \n")
            f.write("    # 设置密码环境变量\n")
            f.write("    env = os.environ.copy()\n")
            f.write("    if args.password:\n")
            f.write("        env['PGPASSWORD'] = args.password\n")
            f.write("    elif 'PGPASSWORD' not in env:\n")
            f.write("        # 如果没有通过参数或环境变量提供密码,则提示输入\n")
            f.write("        password = getpass.getpass('请输入数据库密码(输入后将用于所有导入操作): ')\n")
            f.write("        env['PGPASSWORD'] = password\n")
            f.write("    \n")
            f.write("    # 查找所有SQL文件\n")
            f.write("    current_dir = Path(__file__).parent\n")
            f.write("    sql_files = sorted(current_dir.glob(f'{args.prefix}_part_*.sql'))\n")
            f.write("    \n")
            f.write("    if not sql_files:\n")
            f.write("        print(f'错误: 未找到匹配的SQL文件 ({args.prefix}_part_*.sql)')\n")
            f.write("        sys.exit(1)\n")
            f.write("    \n")
            f.write("    print(f'找到 {len(sql_files)} 个文件')\n")
            f.write("    print(f'数据库: {args.database}')\n")
            f.write("    print(f'并行线程数: {args.workers}')\n")
            f.write("    print('-' * 60)\n")
            f.write("    \n")
            f.write("    # 使用线程池并行导入\n")
            f.write("    success_count = 0\n")
            f.write("    failed_files = []\n")
            f.write("    \n")
            f.write("    with ThreadPoolExecutor(max_workers=args.workers) as executor:\n")
            f.write("        futures = {\n")
            f.write("            executor.submit(import_file, f, args.database, args.user, args.host, args.port, env): f\n")
            f.write("            for f in sql_files\n")
            f.write("        }\n")
            f.write("        \n")
            f.write("        for future in as_completed(futures):\n")
            f.write("            filename, success, error = future.result()\n")
            f.write("            if success:\n")
            f.write("                success_count += 1\n")
            f.write("                print(f'✓ [{success_count}/{len(sql_files)}] {filename}')\n")
            f.write("            else:\n")
            f.write("                failed_files.append((filename, error))\n")
            f.write("                print(f'✗ {filename} - 失败')\n")
            f.write("    \n")
            f.write("    print('-' * 60)\n")
            f.write("    print(f'导入完成: {success_count}/{len(sql_files)} 成功')\n")
            f.write("    \n")
            f.write("    if failed_files:\n")
            f.write("        print(f'\\n失败的文件:')\n")
            f.write("        for filename, error in failed_files:\n")
            f.write("            print(f'  - {filename}')\n")
            f.write("            if error:\n")
            f.write("                print(f'    错误: {error[:200]}')\n")
            f.write("        sys.exit(1)\n\n\n")
            f.write("if __name__ == '__main__':\n")
            f.write("    main()\n")
        
        # 设置Python脚本为可执行
        try:
            os.chmod(py_script, 0o755)
        except:
            pass
        
        print(f"\n已生成导入脚本:")
        print(f"  - Windows: {bat_script.name}")
        print(f"  - Linux/Mac: {sh_script.name}")
        print(f"  - Python并行导入: {py_script.name} (推荐，支持多线程)")


def main():
    parser = argparse.ArgumentParser(
        description='从MySQL dump文件提取INSERT语句，转换为PostgreSQL格式并分割',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  # 基本用法（默认200MB分割）
  python extract_and_split_inserts.py /path/to/mysql_dump.sql /path/to/output_dir
  
  # 自定义分割大小（100MB）
  python extract_and_split_inserts.py /path/to/mysql_dump.sql /path/to/output_dir -s 100
  
  # 自定义输出文件前缀
  python extract_and_split_inserts.py /path/to/mysql_dump.sql /path/to/output_dir -p my_data

注意:
  - 此脚本专门处理INSERT语句，不处理表结构
  - 自动处理Windows换行符（CRLF）
  - 输出文件使用Unix换行符（LF）
  - 生成的文件可以使用多线程并行导入
        """
    )
    
    parser.add_argument('input_file', help='输入的MySQL dump文件路径')
    parser.add_argument('output_dir', help='输出目录路径')
    parser.add_argument('-s', '--size', type=int, default=200, 
                        help='每个分割文件的大小（MB），默认200')
    parser.add_argument('-p', '--prefix', default='pg_inserts',
                        help='输出文件前缀，默认为pg_inserts')
    
    args = parser.parse_args()
    
    # 验证输入文件
    if not os.path.exists(args.input_file):
        print(f"错误: 输入文件不存在: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    
    if not os.path.isfile(args.input_file):
        print(f"错误: 输入路径不是文件: {args.input_file}", file=sys.stderr)
        sys.exit(1)
    
    # 创建提取器并处理
    extractor = InsertExtractorAndSplitter(chunk_size_mb=args.size)
    
    try:
        extractor.process_file(args.input_file, args.output_dir, args.prefix)
    except KeyboardInterrupt:
        print("\n\n用户中断操作", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n错误: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
