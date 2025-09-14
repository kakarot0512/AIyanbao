#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import pandas as pd
import logging
import datetime
import json
import google.generativeai as genai
import time
from functools import wraps
from tqdm import tqdm
import yfinance as yf

# --- Configuration ---
# 设置日志记录，以显示时间戳、日志级别和消息。
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# !!! 重要：请在此处设置您的 Gemini API 密钥。!!!
# 如果没有有效的 API 密钥，脚本将无法工作。
# 您可以将其设置为环境变量 GEMINI_API_KEY，或直接在下面替换 "YOUR_API_KEY_HERE"
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "YOUR_API_KEY_HERE")

# 脚本设计为始终使用 AI 分析。
AI_ANALYSIS_ENABLED = True

if AI_ANALYSIS_ENABLED:
    if not GEMINI_API_KEY or GEMINI_API_KEY == "YOUR_API_KEY_HERE":
        logging.error("错误: Gemini API 密钥未设置。请在脚本中设置 GEMINI_API_KEY 或设置同名环境变量。")
        AI_ANALYSIS_ENABLED = False
    else:
        try:
            genai.configure(api_key=GEMINI_API_KEY)
        except Exception as e:
            logging.error(f"Gemini API 配置失败，请检查你的 API Key: {e}")
            AI_ANALYSIS_ENABLED = False

# --- 目录设置 ---
# 定义输入报告、输出结果和原始数据的目录。
DAILY_REPORT_DIR = "每日报告"
ANALYSIS_RESULT_DIR = "分析结果"
RAW_DATA_DIR = "股票原始数据" # 用于保存雅虎财经原始数据的目录
os.makedirs(ANALYSIS_RESULT_DIR, exist_ok=True)
os.makedirs(RAW_DATA_DIR, exist_ok=True)


# --- 工具函数 ---
def retry(retries=3, delay=5, backoff=2):
    """
    一个带指数退避的重试装饰器，用于网络请求。
    这有助于处理临时的网络问题或API速率限制。
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _retries, _delay = retries, delay
            while _retries > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    _retries -= 1
                    if _retries == 0:
                        logging.warning(f"函数 {func.__name__} 在所有重试后失败: {e}")
                        return None
                    logging.warning(f"函数 {func.__name__} 失败: {e}. 将在 {_delay} 秒后重试... ({_retries} 次重试剩余)")
                    time.sleep(_delay)
                    _delay *= backoff
        return wrapper
    return decorator

# --- 报告解析与数据获取模块 ---
def get_latest_report():
    """查找并返回最新的每日报告的路径。"""
    logging.info("正在搜索最新的每日报告...")
    if not os.path.exists(DAILY_REPORT_DIR):
        logging.error(f"错误: 每日报告目录 '{DAILY_REPORT_DIR}' 未找到。")
        return None
    try:
        date_dirs = [d for d in os.listdir(DAILY_REPORT_DIR) if os.path.isdir(os.path.join(DAILY_REPORT_DIR, d)) and re.match(r'^\d{4}-\d{2}-\d{2}$', d)]
        if not date_dirs:
            logging.error(f"在 '{DAILY_REPORT_DIR}' 中未找到日期格式 (YYYY-MM-DD) 的子目录。")
            return None
        date_dirs.sort(key=lambda d: datetime.datetime.strptime(d, '%Y-%m-%d'), reverse=True)
        
        report_date_dir = os.path.join(DAILY_REPORT_DIR, date_dirs[0])
        report_files = [f for f in os.listdir(report_date_dir) if f.endswith('.md')]
        if not report_files:
            logging.error(f"在目录 '{report_date_dir}' 中未找到 .md 文件。")
            return None
        report_files.sort(reverse=True)
        
        latest_report = os.path.join(report_date_dir, report_files[0])
        logging.info(f"成功找到最新报告: {latest_report}")
        return latest_report
    except Exception as e:
        logging.error(f"查找最新报告时发生严重错误: {e}")
        return None

def extract_stock_recommendations(report_path):
    """从 Markdown 报告中解析并提取推荐的股票列表 (采用更稳健的逐行扫描定位法)。"""
    logging.info(f"正在从报告中提取股票推荐: {report_path}...")
    try:
        with open(report_path, 'r', encoding='utf-8-sig') as f:
            # 预处理：替换不间断空格并移除首尾空格
            lines = [line.replace('\u00A0', ' ').strip() for line in f.readlines()]

        stocks = []
        table_start_index = -1

        # 步骤 1 & 2: 寻找表头和紧随其后的分隔线
        for i, line in enumerate(lines):
            is_header = "股票代码" in line and "公司名称" in line and line.startswith('|')
            if is_header and i + 1 < len(lines):
                next_line = lines[i + 1]
                is_separator = re.match(r'^\s*\|(?:\s*:?---+:?\s*\|)+\s*$', next_line)
                if is_separator:
                    table_start_index = i + 2  # 数据从分隔线之后开始
                    logging.info(f"在第 {i} 行找到表格，数据从第 {table_start_index} 行开始。")
                    break
        
        if table_start_index == -1:
            logging.error("在报告中找不到格式正确的股票推荐表（缺少表头或分隔线）。")
            return None

        # 步骤 3: 从找到的起始位置开始解析数据行
        for i in range(table_start_index, len(lines)):
            line = lines[i]
            if line.startswith('|') and line.endswith('|'):
                cells = [c.strip() for c in line.split('|')[1:-1]]
                
                if len(cells) >= 2:
                    stock_code = cells[0]
                    stock_name = cells[1]
                    
                    if re.match(r'^\d{6}$', stock_code):
                        # 关键检查: 确保公司名称不为空
                        if stock_name:
                            stocks.append({'code': stock_code, 'name': stock_name})
                        else:
                            logging.warning(f"解析到空的股票名称，代码: {stock_code}，行: {line}")
                            # 仍然添加，让后续流程尝试补充
                            stocks.append({'code': stock_code, 'name': ''})
                    else:
                        logging.warning(f"行中发现无效股票代码，假定表格结束: {line}")
                        break
                else:
                    logging.warning(f"跳过列数不足的行: {line}")
            else:
                logging.info(f"在第 {i} 行遇到非表格格式行，停止解析。")
                break
        
        if not stocks:
            logging.error("虽然找到了表格，但未能解析出任何有效的股票数据。")
            return None
            
        logging.info(f"成功提取 {len(stocks)} 支推荐股票。")
        return stocks

    except Exception as e:
        logging.error(f"提取股票推荐时发生错误: {e}")
        return None

def get_yahoo_ticker_symbol(code):
    """为雅虎财经自动添加正确的交易所后缀。"""
    code_str = str(code).strip()
    if code_str.startswith(('60', '68')):
        return f"{code_str}.SS"
    elif code_str.startswith(('00', '30', '20')):
        return f"{code_str}.SZ"
    else:
        logging.warning(f"无法确定股票代码 {code_str} 的交易所。将默认尝试上交所 (.SS)。")
        return f"{code_str}.SS"

@retry(retries=3, delay=10)
def fetch_stock_data_from_yahoo(stock_info):
    """从Yahoo Finance获取一支股票的股价和财务数据，并保存到本地。"""
    code = stock_info['code']
    name = stock_info['name']
    ticker_symbol = get_yahoo_ticker_symbol(code)
    stock_yf = yf.Ticker(ticker_symbol)

    # 如果从报告中提取的名称为空，则尝试从雅虎财经获取作为备用方案
    if not name or not name.strip():
        logging.info(f"股票代码 {code} 的名称为空，正在尝试从雅虎财经获取...")
        try:
            stock_details = stock_yf.info
            updated_name = stock_details.get('longName') or stock_details.get('shortName')
            if updated_name:
                name = updated_name
                stock_info['name'] = updated_name
                logging.info(f"成功为代码 {code} 获取到名称: '{name}'")
            else:
                logging.warning(f"无法从雅虎财经为 {code} 获取到名称。将使用空名称。")
        except Exception as e:
            logging.warning(f"尝试从雅虎财经为 {code} 获取名称时出错: {e}")

    stock_data_package = {"code": code, "name": name}

    stock_raw_data_dir = os.path.join(RAW_DATA_DIR, code)
    os.makedirs(stock_raw_data_dir, exist_ok=True)

    hist_data = stock_yf.history(period="6mo")
    if hist_data.empty:
        logging.warning(f"未能获取 {name}({code}) 的股价历史数据。")
        return None
    
    hist_data.sort_index(ascending=False, inplace=True)
    hist_data.to_csv(os.path.join(stock_raw_data_dir, f"{code}_price_6mo.csv"))
    
    hist_data.index = hist_data.index.strftime('%Y-%m-%d')
    hist_data = hist_data.where(pd.notnull(hist_data), None)
    stock_data_package["price_data"] = hist_data.reset_index().to_dict('records')

    try:
        income_stmt = stock_yf.income_stmt.iloc[:, :4]
        balance_sheet = stock_yf.balance_sheet.iloc[:, :4]
        cash_flow = stock_yf.cashflow.iloc[:, :4]

        income_stmt.to_csv(os.path.join(stock_raw_data_dir, f"{code}_income_4y.csv"))
        balance_sheet.to_csv(os.path.join(stock_raw_data_dir, f"{code}_balance_4y.csv"))
        cash_flow.to_csv(os.path.join(stock_raw_data_dir, f"{code}_cashflow_4y.csv"))

        def format_financial_statement(df):
            df.columns = df.columns.strftime('%Y-%m-%d')
            transposed_df = df.transpose()
            transposed_df = transposed_df.where(pd.notnull(transposed_df), None)
            return transposed_df.reset_index().to_dict('records')

        stock_data_package["income_statement"] = format_financial_statement(income_stmt)
        stock_data_package["balance_sheet"] = format_financial_statement(balance_sheet)
        stock_data_package["cash_flow"] = format_financial_statement(cash_flow)
        
        logging.info(f"已为股票 {code} 保存原始数据于 '{stock_raw_data_dir}'")
    except Exception as e:
        logging.warning(f"获取 {name}({code}) 的财务数据时出错 (可能是数据不完整): {e}。财务数据将为空。")
        stock_data_package["income_statement"] = []
        stock_data_package["balance_sheet"] = []
        stock_data_package["cash_flow"] = []

    return stock_data_package

# --- AI 分析与结果保存模块 ---
@retry(retries=5, delay=10, backoff=2)
def analyze_stocks_in_batch(all_stock_data):
    """
    使用 Gemini AI 模型批量分析股票。
    一次性发送所有股票数据，并期望获得批量响应。
    """
    if not AI_ANALYSIS_ENABLED:
        logging.warning("AI_ANALYSIS_ENABLED 为 False，跳过 AI 分析。")
        return {}

    logging.info(f"check正在使用 Gemini AI 批量分析 {len(all_stock_data)} 支股票...")

    prompt_header = """
# 角色
你是一位经验丰富、一丝不苟的A股股票分析师，擅长通过解读股价走势和财务报表来识别长期价值和潜在风险。

# 任务
根据下面提供的一组股票列表及其从Yahoo Finance获取的原始数据（近半年股价历史和近几年年度财务报表），你必须 **无一例外地** 对列表中的 **每一支** 股票进行综合分析。然后，根据以下核心标准，判断是否应该将其 **排除** 在一个观察列表中：

1. **涨幅巨大，炒作接近泡沫**: 股票价格在短期内（例如过去半年）已经经历了非常巨大的涨幅，并通过考虑未来预估市盈率等估值体系。来确定估值是否过高，存在泡沫风险。
2. **业绩出现过明显问题**: 公司近期内的财务报表存在风险点：如收入和毛利率的大幅下滑，资产负债情况变差，现金流明显减少等

# 特殊情况处理
- **数据不完整**: 如果某支股票的关键财务数据（如利润表、现金流量表）严重缺失，**不要将其排除**。在分析中明确指出数据不完整，并建议用户自行核实。
"""

    prompt_data_section = f"""
# 待分析数据
这是待分析的股票列表及其数据：
```json
{json.dumps(all_stock_data, ensure_ascii=False, indent=2, default=str)}
```
"""
    
    prompt_footer = """
# 分析与输出要求
1.  **严格遵守**: 你必须严格按照上述排除标准和特殊情况处理规则，对输入数据中的 **每一支股票** 进行分析并形成一个JSON对象。
2.  **完整性保证**: 返回的JSON数组 **必须** 包含与输入数据中相同数量的对象。**绝对不要遗漏任何一支股票**。
3.  **强制格式**: 数组中每个对象的结构 **必须** 如下所示。`code` 和 `name` 字段至关重要。
```json
{
  "code": "原始股票代码, 例如 '600519'",
  "name": "公司名称, 例如 '贵州茅台'",
  "should_exclude": boolean,
  "reason": "仅在 should_exclude 为 true 时填写此字段，从'近期明显下跌走势'、'涨幅巨大接近泡沫'、'财务有严重问题'中选择一个。如果should_exclude为false，此字段应为空字符串或null。",
  "analysis": "提供一句话的简明扼要的分析，解释你做出该判断的核心依据。对于数据不完整的股票，should_exclude应为false，并在此处说明‘数据不完整，建议用户自行核实相关财务报表。’"
}
```
4.  **纯净输出**: 确保最终输出是一个格式良好的、完整的JSON数组，前后不要有任何其他文本或Markdown标记。
"""

    full_prompt = prompt_header + prompt_data_section + prompt_footer

    try:
        model = genai.GenerativeModel('gemini-2.5-pro')
        
        logging.info("正在向 Gemini API 发送请求 (这可能需要一些时间)...")
        start_time = time.time()
        response = model.generate_content(full_prompt)
        elapsed_time = time.time() - start_time
        logging.info(f"在 {elapsed_time:.2f} 秒内收到 Gemini API 的响应")
        
        logging.debug(f"收到来自AI的原始响应文本: {response.text[:1000]}...")

        response_text = response.text.strip()
        match = re.search(r'```(json)?\n(.*)\n```', response_text, re.DOTALL)
        if match:
            response_text = match.group(2).strip()
        
        results_list = json.loads(response_text)
        
        if not isinstance(results_list, list):
            logging.error(f"响应格式无效: 期望是列表，但得到 {type(results_list)}")
            return {}
            
        results_map = {str(result.get("code")): result for result in results_list if result.get("code")}
        
        if len(results_map) < len(all_stock_data):
            logging.warning(f"AI响应中遗漏了 {len(all_stock_data) - len(results_map)} 支股票的分析结果。")

        logging.info(f"批量分析完成。收到 {len(results_map)} 支股票的分析结果。")
        return results_map
        
    except json.JSONDecodeError as e:
        logging.error(f"解析JSON响应失败: {e}")
        logging.error(f"失败的响应文本 (前500字符): {response.text[:500]}...")
        return {}
    except Exception as e:
        logging.error(f"批量AI分析期间发生严重错误: {e}")
        return {}

def save_analysis_results(all_stock_data):
    """将最终的分析结果保存为JSON和Markdown文件。"""
    logging.info("正在保存所有分析结果...")
    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    report_data = [{"code": s["code"], "name": s["name"], "analysis_result": s.get("analysis_result", {})} for s in all_stock_data]
    
    json_path = os.path.join(ANALYSIS_RESULT_DIR, f"stock_analysis_{current_time}.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)
    
    md_path = os.path.join(ANALYSIS_RESULT_DIR, f"stock_analysis_{current_time}.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# 股票筛选分析报告 (AI-Powered)\n\n**分析时间:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        included = [s for s in report_data if not s.get('analysis_result', {}).get('should_exclude')]
        excluded = [s for s in report_data if s.get('analysis_result', {}).get('should_exclude')]
        
        f.write(f"## 筛选摘要\n\n- **总分析股票数:** {len(report_data)}\n- **建议保留:** {len(included)}\n- **建议排除:** {len(excluded)}\n\n")
        
        f.write("## 建议保留的股票\n\n")
        if included:
            f.write("| 股票代码 | 公司名称 | AI分析摘要 |\n|:---:|:---:|:---|\n")
            for stock in included:
                analysis_text = stock['analysis_result'].get('analysis', 'N/A')
                if "建议用户自行核实" in analysis_text:
                    analysis_text = f"**{analysis_text}**"
                f.write(f"| {stock['code']} | {stock['name']} | {analysis_text} |\n")
        else:
            f.write("无。\n\n")
        
        f.write("\n## 建议排除的股票\n\n")
        if excluded:
            f.write("| 股票代码 | 公司名称 | 排除原因 | AI分析摘要 |\n|:---:|:---:|:---:|:---|\n")
            for stock in excluded:
                f.write(f"| {stock['code']} | {stock['name']} | **{stock['analysis_result'].get('reason', 'N/A')}** | {stock['analysis_result'].get('analysis', 'N/A')} |\n")
        else:
            f.write("无。\n\n")
    
    logging.info(f"分析结果已保存至 {json_path} 和 {md_path}")
    return md_path

# --- 主函数 ---
def main():
    """主函数，用于编排整个分析工作流。"""
    logging.info("===== 开始股票筛选工作流 (Yahoo Finance + AI批量分析模式) =====")
    
    if not AI_ANALYSIS_ENABLED:
        logging.error("Gemini AI 未启用或配置失败，无法执行分析。程序退出。")
        return

    report_path = get_latest_report()
    if not report_path:
        logging.error("未找到可用的每日报告，程序退出。")
        return
    
    stocks_to_analyze = extract_stock_recommendations(report_path)
    if not stocks_to_analyze:
        logging.error("无法从报告中提取股票列表，程序退出。")
        return
    
    logging.info("\n--- 步骤 1: 从 Yahoo Finance 获取所有股票的数据 ---")
    all_stock_data = []
    
    for stock in tqdm(stocks_to_analyze, desc="获取并保存雅虎财经数据"):
        stock_data_package = fetch_stock_data_from_yahoo(stock)
        if stock_data_package:
            all_stock_data.append(stock_data_package)
        else:
            logging.warning(f"无法获取股票 {stock['code']} 的数据，将在分析中跳过。")

    if not all_stock_data:
        logging.error("未能成功获取任何股票的数据，程序退出。")
        return

    logging.info(f"成功获取了 {len(all_stock_data)} / {len(stocks_to_analyze)} 支股票的数据。")
    
    logging.info("\n--- 步骤 2: 开始 AI 批量分析 ---")
    
    analysis_results_map = analyze_stocks_in_batch(all_stock_data)
    
    error_result = {"should_exclude": True, "reason": "分析失败", "analysis": "未能从AI获取对此股票的有效分析。检查日志中AI的原始响应。"}
    
    # 将分析结果映射回原始数据列表
    for stock_data in all_stock_data:
        stock_code = stock_data['code']
        ai_result = analysis_results_map.get(stock_code, error_result)
        stock_data['analysis_result'] = ai_result

        # --- BUG FIX ---
        # 如果在初始解析和Yahoo查询后名称仍然缺失，则尝试从AI的响应中恢复名称。
        # 这增加了脚本的健壮性，以应对解析失败或数据源问题。
        current_name = stock_data.get("name", "").strip()
        ai_name = ai_result.get("name", "").strip()
        if not current_name and ai_name:
            stock_data["name"] = ai_name
            logging.info(f"为代码 {stock_code} 从 AI 响应中恢复了名称: '{ai_name}'")

    logging.info("已成功映射所有AI分析结果。")
    
    logging.info("\n--- 步骤 3: 保存最终分析报告 ---")
    final_report_path = save_analysis_results(all_stock_data)
    
    logging.info("===== 股票筛选工作流成功完成 =====")
    if final_report_path:
        print("\n" + "="*60)
        print("🎉 分析报告已成功生成！")
        print(f"   Markdown 文件名: {os.path.basename(final_report_path)}")
        print(f"   完整路径: {final_report_path}")
        print("="*60)

if __name__ == "__main__":
    main()

