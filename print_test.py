import inspect

def print_test(var):
    frame       =       inspect.currentframe()
    module_call =       inspect.getouterframes(frame)[1]
    module      =       module_call[0].f_globals['__name__']
    line        =       inspect.currentframe().f_back.f_lineno
    print('\n\n')
    print(f'Control de impresion:\nModulo:{module}\nLinea: {line}\n--------------\n{var}\n--------------\n')
    print('Detener ejecución')
    input()