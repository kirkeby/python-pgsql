__all__ = ['test_timestamp', 'test_unicode', 'test_bytea', 'test_types',
           'test_numeric', 'test_encoding']

import pgsql

class setup_function:
    def __init__(self, module):
        self.module = module
        self.dbapi = pgsql

    def __call__(self):
        self.module.dbapi = self.dbapi
        cnx = self.module.cnx = self.dbapi.connect()
        for sql in getattr(self.module, 'create_statements', []):
            cnx.execute(sql)
        self.module.cu = cnx.cursor()

for module_name in __all__:
    module = getattr(__import__('tests', fromlist=[module_name]), module_name)
    setup = setup_function(module)
    for name, value in module.__dict__.items():
        if name.startswith('test_') and callable(value):
            value.setup = setup

del pgsql, setup_function, setup, module_name, module, name, value
