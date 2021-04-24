import datetime
import os

class Logs:

  '''
  logging
  '''

  PRIORITET = 3

  def __init__(self, nameFile = 'log'):

    '''
    initialize object
    :param nameFile: str file's name
    '''

    if not os.path.exists('LOGS'):
      os.mkdir('LOGS', mode=0o777, dir_fd=None)

    # os.chdir('LOGS')
    self.nameFile = 'LOGS\\' + nameFile + '.log'

  def trace(self, message, prioritet = 0):

    '''
    send message to write in file if prioritet < Logs.PRIORITET
    :param message: str message
    :param prioritet: int message's prioritet, default 0
    '''

    if prioritet <= Logs.PRIORITET:
      mes = f'{datetime.datetime.isoformat(datetime.datetime.now())} {message}'
      self._record(mes)

  def _record(self, message):

    '''
    write message in file
    :param message: str message
    '''

    with open(self.nameFile, 'a') as f:
      f.write(message + '\n')

  def rewriteFile(self):

    '''
     clear log's file
     '''

    with open(self.nameFile, 'w') as f:
      pass

  def print(self):
    '''
    testing function - print contents file
    '''
    with open(self.nameFile, 'r') as f:
      print(f.read())

if __name__ == '__main__':
  Logs.PRIORITET = 1
  log = Logs()
  log.rewriteFile()
  log.trace('hello', 0)
  log.trace('hello1', 1)
  log.trace('hello2', 2)
  log.print()