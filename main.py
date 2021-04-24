import sched
import time
import sender
import getter
import config
import log

class Main_():
    def __init__(self):
        self.log = log.Logs('Main')
        self.log.rewriteFile()
        self.trace('initialasing...', 0)

        settings = config.read_config('config.ini', 'common')
        if settings:
            self.sender_run = False
            if settings['run_sender'] == 'TRUE':
                self.sender_run = True
            self.getter_run = False
            if settings['run_getter'] == 'TRUE':
                self.getter_run = True
            self.delay = int(settings['delay'])
            self.priority = int(settings['priority'])
        else:
            self.trace(f"Can't find 'common' options in config.ini", 0)
            quit()

        self.event_schedule = sched.scheduler(time.time, time.sleep)
        self.sender = sender.Sender()
        self.getter = getter.Getter()

        self.event_schedule.enter(self.delay, self.priority, self.do_something)
        self.event_schedule.run()

    def trace(self, message, levellog):
        self.log.trace(message, levellog)

    def do_something(self):
        self.trace(f'run', 10)

        if self.getter_run:
            self.trace(f'get', 10)
            self.getter.run()
        else:
            self.trace(f'get unavailable from options', 10)

        if self.sender_run:
            self.trace(f'send', 10)
            self.sender.run()
        else:
            self.trace(f'send unavailable from options', 10)
        self.event_schedule.enter(self.delay, self.priority, self.do_something())

if __name__ == '__main__':
    main_ = Main_()