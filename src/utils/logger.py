"""
Módulo de Logging do Pipeline
Configura logging centralizado com arquivos e formatação
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from .config import config


def setup_logger(name: str = 'pipeline', log_file: Optional[str] = None) -> logging.Logger:
    """
    Configura logger com arquivo e console
    
    Args:
        name: Nome do logger
        log_file: Nome do arquivo de log (opcional)
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    # Evitar duplicação de handlers
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, config.get('logging.level', 'INFO')))
    
    # Formato
    log_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter(log_format)
    
    # Handler para console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler para arquivo (se habilitado)
    if config.get('logging.file_logging', True):
        logs_dir = config.logs_dir
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        if log_file is None:
            log_file = config.get('logging.log_file', 'pipeline.log')
        
        # Adicionar timestamp ao nome do arquivo
        timestamp = datetime.now().strftime('%Y%m%d')
        log_path = logs_dir / f"{log_file.replace('.log', '')}_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)  # Arquivo tem mais detalhes
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = None) -> logging.Logger:
    """
    Obtém logger configurado
    
    Args:
        name: Nome do logger (padrão: nome do módulo)
        
    Returns:
        Logger configurado
    """
    if name is None:
        import inspect
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'pipeline')
    
    return setup_logger(name)
