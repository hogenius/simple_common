import logging
from logging.handlers import TimedRotatingFileHandler

class SimpleLogger:
    def __init__(self, name: str, log_file: str = 'app.log', log_level: int = logging.INFO, backup_count: int = 7):
        # 로거 설정
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)
        
        # 시간 기반 파일 핸들러 설정 (하루 단위로 파일 롤링)
        file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=backup_count)
        file_handler.setLevel(log_level)
        
        # 콘솔 핸들러 설정 (필요한 경우)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # 포매터 설정
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 핸들러 추가
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # 초기화 로그 남기기
        self.logger.info("SimpleLogger initialized with name: %s", name)
        
    def log(self, msg: str, log_level: int = logging.INFO):
        # 로그 메시지를 지정된 레벨로 남기기
        if log_level == logging.DEBUG:
            self.logger.debug(msg)
        elif log_level == logging.INFO:
            self.logger.info(msg)
        elif log_level == logging.WARNING:
            self.logger.warning(msg)
        elif log_level == logging.ERROR:
            self.logger.error(msg)
        elif log_level == logging.CRITICAL:
            self.logger.critical(msg)
        else:
            self.logger.info("Unknown log level: %s - %s", log_level, msg)

# 클래스 사용 예시
if __name__ == "__main__":
    my_logger = SimpleLogger(name="TestLogger")
    
    # 로그 메시지와 로그 레벨을 전달하여 로그 남기기
    my_logger.log("This is an info message")
    my_logger.log("This is an error message", logging.ERROR)