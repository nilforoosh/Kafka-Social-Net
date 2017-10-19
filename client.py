import sys
import os
import socket
from threading import Thread
from PyQt5.QtWidgets import *
from pykafka import KafkaClient


###########################################################
def window():

   global text
   global history
   global group
   global win
   global producer
   global consumer
   global content
   global topic
   global t

   content = {}
   producer = {}
   consumer = {}
   topic = {}
   t = {}


   app = QApplication(sys.argv)

   win = QWidget()
   win.setWindowTitle(os.uname()[1])
   win.resize(600,250)
   win.move(400,300)

   history = QTextEdit()
   history.setReadOnly(True)

   text = QLineEdit()
   text.editingFinished.connect(T_Edit_Fin)

   group = QComboBox()
   group.currentTextChanged.connect(G_Changed)


   join = QPushButton('Join to a Group')
   join.clicked.connect(Join_Clicked)

   create = QPushButton('Create a Group')
   create.clicked.connect(Create_Clicked)


   form = QFormLayout()
   form.addRow('History',history)
   form.addRow('Text',text)
   form.addRow('Groups',group)
   form.addRow('',join)
   form.addRow('',create)

	
   win.setLayout(form)
   win.show()

   sys.exit(app.exec_())

##############################################################################	
def T_Edit_Fin():

   tx = text.text()
   if tx != '':
      text.setText('')
      producer[Active_GP].produce(bytes(os.uname()[1] + ': ' + tx,'ASCII'))


##############################################################################
def G_Changed():

   global Active_GP
   Active_GP = group.currentText()
   history.clear()
   history.append(content[Active_GP])


###########################################################################################
def Join_Clicked():

   name , ok = QInputDialog.getText(win,'Join to a Group','please enter the group name:')
   if ok:
      content[name] = ''
      topic[name] = client.topics[bytes(name,'ASCII')]
      producer[name] = topic[name].get_sync_producer()
      t[name] = Thread(target=Consume,args=(name,))
      t[name].start()
      group.addItem(name)


###########################################################################################
def Create_Clicked():

   name , ok = QInputDialog.getText(win,'Create a Group','please enter the group name:')
   if ok:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

      host = '172.16.130.1'
      port = 3030

      sock.connect((host,port))
      sock.send(bytes(name,'ASCII'))
      sock.close()


###########################################################################################
def Consume(name):

   consumer[name] = topic[name].get_simple_consumer()
   for msg in consumer[name]:
      content[name] += msg.value.decode('ASCII')+'\n'
      if Active_GP == name:
         history.append(msg.value.decode('ASCII'))


###################################################################
if __name__ == '__main__':

   client = KafkaClient(hosts='172.16.130.1:9092')
   window()
   