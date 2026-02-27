# -*- coding: utf-8 -*-
"""
===================================
定时调度模块
===================================

职责：
1. 支持每日定时执行股票分析
2. 支持定时执行大盘复盘
3. 优雅处理信号，确保可靠退出
4. 支持多个定时时间点，不同时间执行不同任务

依赖：
- schedule: 轻量级定时任务库
"""

import logging
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Callable, Optional, List, Dict

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """
    优雅退出处理器
    
    捕获 SIGTERM/SIGINT 信号，确保任务完成后再退出
    """
    
    def __init__(self):
        self.shutdown_requested = False
        self._lock = threading.Lock()
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理函数"""
        with self._lock:
            if not self.shutdown_requested:
                logger.info(f"收到退出信号 ({signum})，等待当前任务完成...")
                self.shutdown_requested = True
    
    @property
    def should_shutdown(self) -> bool:
        """检查是否应该退出"""
        with self._lock:
            return self.shutdown_requested


class Scheduler:
    """
    定时任务调度器
    
    基于 schedule 库实现，支持：
    - 每日定时执行
    - 多个定时时间点
    - 启动时立即执行
    - 优雅退出
    """
    
    def __init__(self, schedule_times: str = "18:00", include_market_review: bool = True):
        """
        初始化调度器
        
        Args:
            schedule_times: 每日执行时间，格式 "HH:MM" 或 "HH:MM,HH:MM"（多个时间用逗号分隔）
            include_market_review: 是否包含大盘复盘（仅在最后一个时间点执行）
        """
        try:
            import schedule
            self.schedule = schedule
        except ImportError:
            logger.error("schedule 库未安装，请执行: pip install schedule")
            raise ImportError("请安装 schedule 库: pip install schedule")
        
        self.schedule_times = schedule_times
        self.include_market_review = include_market_review
        self.shutdown_handler = GracefulShutdown()
        self._task_callbacks: Dict[str, Callable] = {}
        self._running = False
        
        # 解析时间列表
        self._time_list = []
        for t in schedule_times.split(','):
            t = t.strip()
            if t:
                # 补齐小时为两位数
                if ':' in t:
                    parts = t.split(':')
                    if len(parts[0]) == 1:
                        parts[0] = '0' + parts[0]
                    t = ':'.join(parts)
                self._time_list.append(t)
        
    def set_task(self, time_point: str, task: Callable, run_immediately: bool = False):
        """
        设置指定时间的任务
        
        Args:
            time_point: 时间点，格式 "HH:MM"
            task: 要执行的任务函数（无参数）
            run_immediately: 是否在设置后立即执行一次
        """
        # 补齐小时为两位数
        formatted_time = time_point
        if ':' in time_point:
            parts = time_point.split(':')
            if len(parts[0]) == 1:
                parts[0] = '0' + parts[0]
                formatted_time = ':'.join(parts)
        
        self._task_callbacks[formatted_time] = task
        
        # 设置每日定时任务
        self.schedule.every().day.at(formatted_time).do(self._create_safe_task(formatted_time))
        logger.info(f"已设置定时任务，执行时间: {formatted_time}")
        
        if run_immediately:
            logger.info(f"立即执行任务: {time_point}...")
            task()
    
    def _create_safe_task(self, time_point: str) -> Callable:
        """创建指定时间点的安全执行任务"""
        def safe_task():
            if time_point in self._task_callbacks:
                callback = self._task_callbacks[time_point]
                try:
                    logger.info("=" * 50)
                    logger.info(f"定时任务开始执行 - {time_point} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info("=" * 50)
                    
                    callback()
                    
                    logger.info(f"定时任务执行完成 - {time_point} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                except Exception as e:
                    logger.exception(f"定时任务执行失败 ({time_point}): {e}")
            else:
                logger.warning(f"未找到时间点 {time_point} 的任务回调")
        return safe_task
    
    def set_daily_task(self, task: Callable, run_immediately: bool = True):
        """
        设置每日定时任务（兼容旧接口）
        
        Args:
            task: 要执行的任务函数（无参数）
            run_immediately: 是否在设置后立即执行一次
        """
        # 使用第一个时间点
        if self._time_list:
            self.set_task(self._time_list[0], task, run_immediately)
    
    def run(self):
        """
        运行调度器主循环
        
        阻塞运行，直到收到退出信号
        """
        self._running = True
        logger.info("调度器开始运行...")
        logger.info(f"执行时间点: {self._time_list}")
        logger.info(f"下次执行时间: {self._get_next_run_time()}")
        
        while self._running and not self.shutdown_handler.should_shutdown:
            self.schedule.run_pending()
            time.sleep(30)  # 每30秒检查一次
            
            # 每小时打印一次心跳
            if datetime.now().minute == 0 and datetime.now().second < 30:
                logger.info(f"调度器运行中... 下次执行: {self._get_next_run_time()}")
        
        logger.info("调度器已停止")
    
    def _get_next_run_time(self) -> str:
        """获取下次执行时间"""
        jobs = self.schedule.get_jobs()
        if jobs:
            next_runs = [job.next_run for job in jobs if job.next_run]
            if next_runs:
                next_run = min(next_runs)
                return next_run.strftime('%Y-%m-%d %H:%M:%S')
        return "未设置"
    
    def stop(self):
        """停止调度器"""
        self._running = False


def run_with_schedule(
    task: Callable,
    schedule_times: str = "18:00",
    run_immediately: bool = True,
    include_market_review: bool = True
):
    """
    便捷函数：使用定时调度运行任务
    
    Args:
        task: 要执行的任务函数
        schedule_times: 每日执行时间，支持多个时间点用逗号分隔，如 "9:00,18:00"
        run_immediately: 是否立即执行一次（默认执行第一个时间点的任务）
        include_market_review: 是否包含大盘复盘（仅在最后一个时间点执行）
    """
    scheduler = Scheduler(schedule_times=schedule_times, include_market_review=include_market_review)
    scheduler.set_daily_task(task, run_immediately=run_immediately)
    scheduler.run()


if __name__ == "__main__":
    # 测试定时调度
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
    )
    
    def test_task():
        print(f"任务执行中... {datetime.now()}")
        time.sleep(2)
        print("任务完成!")
    
    print("启动测试调度器（按 Ctrl+C 退出）")
    run_with_schedule(test_task, schedule_times="23:59", run_immediately=True)
