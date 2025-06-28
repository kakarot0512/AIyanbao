#!/usr/bin/env python
# -*- coding: utf-8 -*-
# cls_spider.py - 财联社电报爬虫

import json
import time
from datetime import datetime, timedelta
import os
import sys
from pathlib import Path
import re
import hashlib
import urllib.parse
from typing import Optional, List, Dict, Set

import requests
import pytz

# --- 1. 配置常量 ---
CONFIG = {
    "OUTPUT_DIR": "./财联社/output/cls",  # 输出目录
    "MAX_TELEGRAMS_FETCH": 100,  # 每次API请求最大获取电报数量 (根据财联社API实际能力调整)
    "RED_KEYWORDS": ["利好", "利空", "重要", "突发", "紧急", "关注", "提醒", "涨停", "大跌", "突破"],  # 标红关键词，可扩展
    "FILE_SEPARATOR": "━━━━━━━━━━━━━━━━━━━",  # 文件内容分割线
    "USE_PROXY": os.getenv("USE_PROXY", "False").lower() == "true",
    "DEFAULT_PROXY": os.getenv("DEFAULT_PROXY", "http://127.0.0.1:10086"),
    "REQUEST_TIMEOUT": 15, # 请求超时时间
    "RETRY_ATTEMPTS": 3, # 请求重试次数
    "RETRY_DELAY": 5, # 重试间隔秒数
}

# 创建输出目录
os.makedirs(CONFIG["OUTPUT_DIR"], exist_ok=True)

# --- 2. 时间处理工具类 ---
class TimeHelper:
    """提供时间相关的辅助方法"""
    BEIJING_TZ = pytz.timezone("Asia/Shanghai")
    
    @staticmethod
    def get_beijing_time() -> datetime: 
        return datetime.now(TimeHelper.BEIJING_TZ)
    
    @staticmethod
    def format_date(dt: datetime = None) -> str: 
        return (dt or TimeHelper.get_beijing_time()).strftime("%Y年%m月%d日")
    
    @staticmethod
    def format_time(dt: datetime = None) -> str: 
        return (dt or TimeHelper.get_beijing_time()).strftime("%H:%M:%S")
    
    @staticmethod
    def format_datetime(dt: datetime = None) -> str: 
        return (dt or TimeHelper.get_beijing_time()).strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def timestamp_to_beijing_datetime(timestamp: int) -> datetime: 
        return datetime.fromtimestamp(timestamp, TimeHelper.BEIJING_TZ)
    
    @staticmethod
    def timestamp_to_hhmm(timestamp: int) -> str:
        try: 
            return TimeHelper.timestamp_to_beijing_datetime(timestamp).strftime("%H:%M")
        except (ValueError, TypeError): 
            return ""
    
    @staticmethod
    def get_week_start_end(dt: datetime = None) -> tuple:
        """获取包含指定日期的星期的起始日期和结束日期"""
        dt = dt or TimeHelper.get_beijing_time()
        # 获取当前是一周中的第几天 (0是周一，6是周日)
        weekday = dt.weekday()
        # 计算本周的开始日期（周一）和结束日期（周日）
        week_start = dt - timedelta(days=weekday)
        week_end = week_start + timedelta(days=6)
        return week_start.replace(hour=0, minute=0, second=0, microsecond=0), week_end.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    @staticmethod
    def get_week_string(dt: datetime = None) -> str:
        """获取包含指定日期的星期的字符串表示，格式为 'YYYY-WW'"""
        dt = dt or TimeHelper.get_beijing_time()
        # 获取年份和周数
        year, week_num, _ = dt.isocalendar()
        return f"{year}-W{week_num:02d}"

# --- 3. 财联社 API 交互类 ---
class CailianpressAPI:
    """处理财联社电报数据的获取和解析"""
    BASE_URL = "https://www.cls.cn/nodeapi/updateTelegraphList"
    APP_PARAMS = {"app_name": "CailianpressWeb", "os": "web", "sv": "7.7.5"}
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    
    @staticmethod
    def _generate_signature(params: dict) -> str:
        sorted_keys = sorted(params.keys())
        params_string = "&".join([f"{key}={params[key]}" for key in sorted_keys])
        sha1_hash = hashlib.sha1(params_string.encode('utf-8')).hexdigest()
        return hashlib.md5(sha1_hash.encode('utf-8')).hexdigest()
    
    @staticmethod
    def _get_request_params() -> dict:
        all_params = {**CailianpressAPI.APP_PARAMS}
        all_params["sign"] = CailianpressAPI._generate_signature(all_params)
        return all_params
    
    @staticmethod
    def fetch_telegrams() -> list[dict]:
        params = CailianpressAPI._get_request_params()
        full_url = f"{CailianpressAPI.BASE_URL}?{urllib.parse.urlencode(params)}"
        proxies = {"http": CONFIG["DEFAULT_PROXY"], "https": CONFIG["DEFAULT_PROXY"]} if CONFIG["USE_PROXY"] else None
        print(f"[{TimeHelper.format_datetime()}] 正在请求财联社API...")
        for attempt in range(CONFIG["RETRY_ATTEMPTS"]):
            try:
                response = requests.get(full_url, proxies=proxies, headers=CailianpressAPI.HEADERS, timeout=CONFIG["REQUEST_TIMEOUT"])
                response.raise_for_status()
                data = response.json()
                if data.get("error") == 0 and data.get("data") and data["data"].get("roll_data"):
                    raw_telegrams = data["data"]["roll_data"]
                    print(f"[{TimeHelper.format_datetime()}] 成功获取 {len(raw_telegrams)} 条原始财联社电报。")
                    processed = []
                    for item in raw_telegrams:
                        if item.get("is_ad"): continue
                        item_id = str(item.get("id"))
                        title = item.get("title", "")
                        content = item.get("brief", "") or title
                        timestamp = item.get("ctime")
                        item_time_str, ts_int = "", None
                        if timestamp:
                            try:
                                ts_int = int(timestamp)
                                item_time_str = TimeHelper.timestamp_to_hhmm(ts_int)
                            except (ValueError, TypeError): pass
                        processed.append({
                            "id": item_id, 
                            "content": content, 
                            "time": item_time_str,
                            "url": f"https://www.cls.cn/detail/{item_id}" if item_id else "",
                            "is_red": any(k in (title + content) for k in CONFIG["RED_KEYWORDS"]),
                            "timestamp_raw": ts_int,
                            # 添加一个内容哈希用于后续去重
                            "content_hash": hashlib.md5(content.encode('utf-8')).hexdigest()
                        })
                    return processed
            except requests.exceptions.RequestException as e: 
                print(f"[{TimeHelper.format_datetime()}] 请求API失败 (尝试 {attempt + 1}): {e}")
            except json.JSONDecodeError as e: 
                print(f"[{TimeHelper.format_datetime()}] JSON解析失败 (尝试 {attempt + 1}): {e}")
            if attempt < CONFIG["RETRY_ATTEMPTS"] - 1: 
                time.sleep(CONFIG["RETRY_DELAY"])
        return []

# --- 4. 文件写入与读取类 (按周组织) ---
class TelegramFileManager:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, week_str: str) -> Path:
        """根据周字符串获取文件路径"""
        return self.output_dir / f"cls_{week_str}.md"

    def get_existing_content_hashes(self, week_str: str) -> tuple:
        """获取文件中已存在的内容哈希集合，用于去重"""
        file_path = self._get_file_path(week_str)
        if not file_path.exists():
            return set(), set()
        
        # 读取文件内容
        content = file_path.read_text(encoding="utf-8")
        
        # 提取所有内容，用于计算哈希值进行去重
        # 从每行中提取实际内容部分（去除时间戳和URL）
        content_lines = re.findall(r'\[\d{2}:\d{2}\]\s+(?:\*\*\[|\[)([^\]]+)', content)
        
        # 计算每行内容的哈希值
        content_hashes = {hashlib.md5(line.encode('utf-8')).hexdigest() for line in content_lines if line}
        
        # 同时也获取ID用于辅助去重
        ids = set(re.findall(r'\(https://www.cls.cn/detail/(\d+)\)', content))
        
        return content_hashes, ids

    def _format_telegram_lines_for_insertion(self, telegram: dict, day_str: str) -> List[str]:
        """将单条电报格式化为要插入文件的文本行列表，包含日期信息"""
        title = telegram.get("content", "")
        time_str = telegram.get("time", "")
        url = telegram.get("url", "")
        is_red = telegram.get("is_red", False)
        
        line = ""
        if url:
            if is_red: 
                line = f"  - [{day_str} {time_str}] **[{title}]({url})**"
            else: 
                line = f"  - [{day_str} {time_str}] [{title}]({url})"
        else: # Fallback
            if is_red: 
                line = f"  - [{day_str} {time_str}] **{title}**"
            else: 
                line = f"  - [{day_str} {time_str}] {title}"
        
        return [line, ""] # 返回内容行和紧随其后的一个空行

    def append_new_telegrams(self, new_telegrams: List[dict]) -> bool:
        """
        核心方法：将新电报追加到对应的周文件中，按内容去重
        """
        if not new_telegrams:
            print(f"[{TimeHelper.format_datetime()}] 没有新电报需要保存到文件。")
            return False

        # 按时间倒序排列新电报，确保最新的在最前面
        new_telegrams.sort(key=lambda x: x.get("timestamp_raw", 0), reverse=True)
        
        # 按周对新电报进行分组
        telegrams_by_week = {}
        for t in new_telegrams:
            if not t.get("timestamp_raw"): continue
            dt = TimeHelper.timestamp_to_beijing_datetime(t["timestamp_raw"])
            week_str = TimeHelper.get_week_string(dt)
            day_str = dt.strftime("%m-%d")  # 添加日期信息，格式为 MM-DD
            
            if week_str not in telegrams_by_week: 
                telegrams_by_week[week_str] = []
            
            # 添加日期信息到电报对象
            t["day_str"] = day_str
            telegrams_by_week[week_str].append(t)

        saved_any_new = False
        for week_str, items_for_week in telegrams_by_week.items():
            file_path = self._get_file_path(week_str)
            
            # 获取已存在的内容哈希和ID，用于去重
            existing_content_hashes, existing_ids = self.get_existing_content_hashes(week_str)
            
            # 过滤掉内容重复的电报
            filtered_items = []
            for item in items_for_week:
                content_hash = item.get("content_hash")
                item_id = item.get("id")
                
                # 如果内容哈希或ID已存在，则跳过
                if content_hash in existing_content_hashes or (item_id and item_id in existing_ids):
                    continue
                
                filtered_items.append(item)
                # 更新已存在集合，避免同一批次中的重复
                existing_content_hashes.add(content_hash)
                if item_id:
                    existing_ids.add(item_id)
            
            if not filtered_items:
                print(f"[{TimeHelper.format_datetime()}] 周 {week_str} 没有新的非重复电报。")
                continue
                
            # 分类电报
            new_red = [t for t in filtered_items if t.get("is_red")]
            new_normal = [t for t in filtered_items if not t.get("is_red")]

            # 将新电报格式化为待插入的行
            new_red_lines = []
            for t in new_red:
                new_red_lines.extend(self._format_telegram_lines_for_insertion(t, t.get("day_str", "")))
                
            new_normal_lines = []
            for t in new_normal:
                new_normal_lines.extend(self._format_telegram_lines_for_insertion(t, t.get("day_str", "")))

            # 读取现有文件或创建模板
            if file_path.exists():
                lines = file_path.read_text(encoding="utf-8").split('\n')
            else:
                week_start, week_end = TimeHelper.get_week_start_end(
                    datetime.strptime(f"{week_str.split('-W')[0]}-{week_str.split('W')[1]}-1", "%Y-%W-%w")
                )
                week_title = f"# 财联社周报 ({week_start.strftime('%Y-%m-%d')} 至 {week_end.strftime('%Y-%m-%d')})"
                lines = [week_title, "", "**🔴 重要电报**", "", CONFIG["FILE_SEPARATOR"], "", "**📰 一般电报**", ""]
            
            # 插入"一般电报"
            if new_normal_lines:
                try:
                    idx = lines.index("**📰 一般电报**") + 1
                    # 在标题行和第一条内容间插入一个空行（如果需要）
                    if idx < len(lines) and lines[idx].strip() != "": 
                        lines.insert(idx, "")
                    lines[idx+1:idx+1] = new_normal_lines
                    saved_any_new = True
                except ValueError: # 如果标题不存在，则在末尾追加
                    lines.extend(["", CONFIG["FILE_SEPARATOR"], "", "**📰 一般电报**", ""])
                    lines.extend(new_normal_lines)

            # 插入"重要电报"
            if new_red_lines:
                try:
                    idx = lines.index("**🔴 重要电报**") + 1
                    if idx < len(lines) and lines[idx].strip() != "": 
                        lines.insert(idx, "")
                    lines[idx+1:idx+1] = new_red_lines
                    saved_any_new = True
                except ValueError: # 如果标题不存在，则在开头追加
                    lines.insert(0, "**🔴 重要电报**")
                    lines.insert(1, "")
                    lines[2:2] = new_red_lines

            # 将更新后的内容写回文件
            try:
                file_path.write_text("\n".join(lines), encoding="utf-8")
                print(f"[{TimeHelper.format_datetime()}] 已将 {len(filtered_items)} 条新电报追加到文件: {file_path}")
            except Exception as e:
                print(f"[{TimeHelper.format_datetime()}] 写入文件失败: {e}")

        return saved_any_new

# --- 6. 主程序逻辑 ---
def main():
    """主函数，编排整个爬取和保存流程"""
    print(f"\n--- 财联社电报抓取程序启动 --- [{TimeHelper.format_datetime()}]")

    file_manager = TelegramFileManager(CONFIG["OUTPUT_DIR"])

    # 1. 获取财联社电报
    fetched_telegrams = CailianpressAPI.fetch_telegrams()
    if not fetched_telegrams:
        print(f"[{TimeHelper.format_datetime()}] 未获取到任何财联社电报，程序退出。")
        return

    # 2. 将新电报追加到文件（内部会进行内容去重）
    file_manager.append_new_telegrams(fetched_telegrams)

    print(f"--- 财联社电报抓取程序完成 --- [{TimeHelper.format_datetime()}]\n")

if __name__ == "__main__":
    main() 