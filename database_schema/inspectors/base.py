# database_schema/base.py
from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.exc import (
    OperationalError,
    ArgumentError,
    NoSuchModuleError,
    TimeoutError
)
from urllib.parse import quote_plus

class BaseInspector(ABC):
    """元数据获取抽象基类"""
    
    def __init__(self, host: str, port: int, database: str, 
                username: str, password: str, schema_name: str = None, **kwargs):
        try:
            self.engine = create_engine(
                self.build_conn_str(host, port, database, username, password)
            )
            self.conn = self.engine.connect()
        except ArgumentError as e:
            raise ValueError(f"连接字符串格式错误: {str(e)}")
        except NoSuchModuleError as e:
            raise ValueError(f"驱动未安装")
        except OperationalError as e:
            '''
                这里可以捕获uesr_name,database_name,password 抛给用户
                但是不能捕获IP, Port 会被 Dify 捕获。
                
                core/server/tcp/request_render.py
                raise Exception("Connection is closed")
            '''
            raise ValueError(f"无法连接到数据库, 请检查数据库名称，用户名，密码")
        except TimeoutError as e:
            raise ValueError(f"请检查IP/Port")
        except Exception as e:
            raise ValueError(f"建立数据库连接时发生错误: {str(e)}")
        self.schema_name = schema_name  # 所有子类都会继承这个属性
    
    @abstractmethod
    def build_conn_str(self, host: str, port: int, database: str,
                     username: str, password: str) -> str:
        """构造数据库连接字符串"""
        pass
    
    @abstractmethod
    def get_table_names(self, inspector: reflection.Inspector) -> list[str]:
        """获取所有表名"""
        pass
    
    @abstractmethod
    def get_table_comment(self, inspector: reflection.Inspector, 
                        table_name: str) -> str:
        """获取表注释"""
        pass
    
    @abstractmethod
    def get_column_comment(self, inspector: reflection.Inspector,
                         table_name: str, column_name: str) -> str:
        """获取列注释"""
        pass
    
    @abstractmethod
    def normalize_type(self, raw_type: str) -> str:
        """标准化字段类型"""
        pass