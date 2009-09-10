import os
__all__ = [filename.replace('.py', '')
           for filename in os.listdir(os.path.dirname(__file__))
           if filename.startswith('test_') and filename.endswith('.py')]

import pgsql

class fixtures:
    def __init__(self, module):
        self.module = module
        self.dbapi = pgsql

    def setup(self):
        self.module.dbapi = self.dbapi
        cnx = self.module.cnx = self.dbapi.connect()
        for sql in getattr(self.module, 'create_statements', []):
            cnx.execute(sql)
        self.module.cu = cnx.cursor()

    def teardown(self):
        self.module.cu.close()
        self.module.cnx.close()

for module_name in __all__:
    module = getattr(__import__('tests', fromlist=[module_name]), module_name)
    fix = fixtures(module)
    for name, value in module.__dict__.items():
        if name.startswith('test_') and callable(value):
            value.setup = fix.setup
            value.teardown = fix.teardown

del os, pgsql, fix, module_name, module, name, value
