c = get_config()

c.NotebookApp.tornado_settings = {
    'headers': {
        'Content-Security-Policy': ''
    }
}

c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False

c.Application.verbose_crash = True

c.KernelSpecManager.whitelist = {'python2'}
