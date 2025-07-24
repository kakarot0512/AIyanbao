#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import logging
from datetime import datetime, timedelta

# --- 配置 ---
# 配置日志记录，使其输出格式更清晰
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 要清理的目标目录
TARGET_DIR = "Gemini发送内容"
# 要保留的天数
DAYS_TO_KEEP = 10

def clean_old_subfolders():
    """
    扫描指定目录，并删除创建时间早于指定天数的子文件夹。
    子文件夹的名称必须是 'YYYY-MM-DD' 格式。
    """
    logging.info(f"开始清理 '{TARGET_DIR}' 目录下的旧文件夹...")
    logging.info(f"将删除 {DAYS_TO_KEEP} 天前的文件夹。")

    # 检查目标目录是否存在
    if not os.path.isdir(TARGET_DIR):
        logging.warning(f"目标目录 '{TARGET_DIR}' 不存在。脚本将退出。")
        return

    # 计算截止日期，任何比这个日期早的文件夹都将被删除
    cutoff_date = datetime.now() - timedelta(days=DAYS_TO_KEEP)
    logging.info(f"截止日期: 早于 {cutoff_date.strftime('%Y-%m-%d')} 的文件夹将被删除。")

    deleted_count = 0
    kept_count = 0

    # 遍历目标目录下的所有项目
    for item_name in os.listdir(TARGET_DIR):
        item_path = os.path.join(TARGET_DIR, item_name)

        # 确保它是一个目录
        if os.path.isdir(item_path):
            try:
                # 尝试将文件夹名称解析为日期
                # 脚本假设文件夹名称格式为 'YYYY-MM-DD'
                folder_date = datetime.strptime(item_name, '%Y-%m-%d')

                # 如果文件夹日期早于截止日期，则删除它
                if folder_date < cutoff_date:
                    logging.info(f"正在删除旧文件夹: {item_path} (日期: {item_name})")
                    shutil.rmtree(item_path) # 使用 shutil.rmtree 删除整个目录及其内容
                    deleted_count += 1
                else:
                    logging.info(f"保留近期文件夹: {item_path} (日期: {item_name})")
                    kept_count += 1
            except ValueError:
                # 如果文件夹名称不是 'YYYY-MM-DD' 格式，则跳过
                logging.warning(f"跳过格式不正确的文件夹: {item_path}")
            except Exception as e:
                logging.error(f"删除文件夹 {item_path} 时发生错误: {e}")

    logging.info("清理完成。")
    logging.info(f"总计: {deleted_count} 个文件夹被删除，{kept_count} 个文件夹被保留。")

if __name__ == "__main__":
    clean_old_subfolders()