# logger_config.py
import logging
import time
import os

LogPath = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')  # 存放log的路径

# 判断日志文件夹是否存在，不存则创建
if not os.path.exists(LogPath):
    os.makedirs(LogPath)

# 缓存已创建的 logger
_logger_cache = {}

# 标志变量：用于追踪是否使用过默认logger
_default_logger_initialized = False


def get_logger(log_name=None):
    """
    获取配置好的 logger 对象。每个 log_name 对应一个独立的 logger。
    
    使用规则：
    - 如果指定了 log_name，则使用指定的日志文件，不会创建默认日志
    - 如果 log_name 为 None 或空字符串，则使用默认的日期-based 文件名
    
    Args:
        log_name: 日志文件名（可选）。示例: "task.log", "task_a.log" 等
                  如果为 None，则使用默认文件名 app_YYYY_MM_DD.log
    
    Returns:
        logging.Logger: 配置好的 logger 对象
    
    Examples:
        # 使用自定义文件名 task.log
        logger = get_logger("task.log")
        logger.info("这只会写入 task.log")
        
        # 使用默认文件名 app_YYYY_MM_DD.log
        logger = get_logger()
        logger.info("这会写入默认日志文件")
    """
    
    # 确定日志文件名
    if not log_name:
        # 使用默认的日期-based 文件名
        log_name = f"app_{time.strftime('%Y-%m-%d', time.localtime()).replace('-', '_')}.log"
    
    # 如果已缓存该日志文件的 logger，直接返回
    if log_name in _logger_cache:
        return _logger_cache[log_name]
    
    # 创建新的 logger（使用完整的日志文件路径作为logger名字，确保唯一性）
    log_path = os.path.join(LogPath, log_name)
    logger = logging.getLogger(log_name)
    
    # 重要：检查logger是否已有处理器，如果有则说明已被初始化，直接返回
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)
    
    # 禁用日志传播，确保日志不会流向父logger或根logger
    logger.propagate = False
    
    # 创建文件处理器
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建日志格式
    file_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y%m%d %H:%M:%S'
    )
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(file_format)
    console_handler.setFormatter(console_format)
    
    # 添加处理器到 logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 缓存 logger
    _logger_cache[log_name] = logger
    
    return logger

