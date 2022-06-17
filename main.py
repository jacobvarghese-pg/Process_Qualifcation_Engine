# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'main.ui'
#
# Created by: PyQt5 UI code generator 5.15.4
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.


from time import sleep
from PyQt5 import QtCore, QtGui, QtWidgets
from mainui import Ui_MainWindow
from process_analyser import get_data, save_data_to_file, initialize_db
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg, NavigationToolbar2QT as Navi
from PyQt5.QtGui import QIcon, QPixmap
from PyQt5.QtWidgets import QApplication, QWidget, QLabel

from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *


class Main(Ui_MainWindow):
    def __init__(self) -> None:
        super().__init__()

class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super(Worker, self).__init__()
        # Store constructor arguments (re-used for processing)
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    @pyqtSlot()
    def run(self):
        '''
        Initialise the runner function with passed args, kwargs.
        '''
        self.fn(*self.args, **self.kwargs)

class RequiredParams():
    def __init__(self, start_time, end_time, line, filter_name, schema) -> None:
        self.start_time = start_time
        self.end_time = end_time
        self.line = line
        self.filter_name = filter_name
        self.schema = schema

    def __str__(self) -> str:
        return ",".join([str(self.start_time), str(self.end_time), self.line, self.filter_name, self.schema])

class Utils():
    def __init__(self, ui: Ui_MainWindow) -> None:
        self.ui = ui
        self.filename = ""
        self.filenames = []
        self.current_index = 0
        self.progress= 0

    def next_image(self):
        self.current_index = self.current_index + 1
        self.current_index = self.current_index % len(self.filenames)
        self.show_images()

    def previous_image(self):
        self.current_index = self.current_index - 1
        self.current_index = self.current_index % len(self.filenames)
        self.show_images()

    def get_selected_item_or_first_item(self, field):
        if len(field.selectedItems()) > 0:
            line = field.selectedItems()[0].text()
        else:
            line = field.item(0).text()

        return line
    
    def fetch_all_parameters(self):
        start_time = ui.Start_Time.dateTime().toSecsSinceEpoch()
        end_time = ui.End_TIme.dateTime().toSecsSinceEpoch()

        schema = "high_definition"
        line = self.get_selected_item_or_first_item(ui.Line_Select)
        parameter_group = self.get_selected_item_or_first_item(ui.Select_Tags)
        params = RequiredParams(start_time, end_time, line, parameter_group, schema)

        self.threadpool = QThreadPool()

        worker = Worker(self.download_data, params) # Any other args, kwargs are passed to the run function
        self.threadpool.start(worker)

        # Start progress bar
        ui.progressBar.setMaximum(100)
        ui.progressBar.setValue(10)

        #self.download_data(params)
        return params

    def download_data(self, required_params: RequiredParams):
        import pandas as pd
        from datetime import datetime
        initialize_db()
        line = required_params.line
        sheet = required_params.filter_name
        df_excel = pd.read_excel(self.filename, sheet_name=sheet)
        measures = df_excel['Tags']
 
        start = required_params.start_time
        end = required_params.end_time
    
        total_periods = int((end - start)/(3600)) # gives the number of 1 hr period in this time frame
        df_to_save_to_file = pd.DataFrame()
        current_measure_index = 0
    
        for measure in measures:
            current_measure_index = current_measure_index + 1
            df = pd.DataFrame()
            for i in range(total_periods):
                df['Tag']=measure
                d = get_data(line, schema='high_definition', filter_name = sheet ,measure = measure, start_time = (start + i*8*3600), end_time = (start + (i+1)*8*3600))
                df = df.append(d)
                df = df.reset_index(drop = True)

            (avg_plot, max_plot) = save_data_to_file(measure, df)    
            # un comment this to save one by one
            #df.to_excel(measure+".xlsx")
            self.filenames.append((avg_plot, max_plot))
            self.progress = current_measure_index * 100 / len(measures)
            #ui.progressBar.setValue(current_measure_index * 100 / len(measures))

        df_to_save_to_file = df_to_save_to_file.append(df)
        # Hardcoded name
        df_to_save_to_file.to_excel("badboihtr109.xlsx")
        print("Finished downloading all files")
        #ui.progressBar.setValue(i * 100 / total_periods)


    def show_images(self):
        if (len(self.filenames) == 0):
            return 
        f = self.filenames[self.current_index]
        (avg_plot, max_plot) = f
        pixmap_avg = QPixmap(avg_plot)
        ui.main_analysis.setPixmap(pixmap_avg)
        ui.main_analysis.setScaledContents(True)
        ui.main_analysis.resize(pixmap_avg.width(), pixmap_avg.height())

        pixmap_max = QPixmap(max_plot)
        ui.anomaly_analysis.setPixmap(pixmap_max)
        ui.anomaly_analysis.setScaledContents(True)
        ui.anomaly_analysis.resize(pixmap_max.width(), pixmap_max.height())
        self.ui.progressBar.setValue(self.progress)

    def populate_parameter_groups(self):
        import pandas as pd
        file_name = QtWidgets.QFileDialog.getOpenFileName(caption="Open Excel File", filter="Excel Files (*.xlsx *.xls)")
        df_excel = pd.ExcelFile(file_name[0])
        self.filename = file_name[0]
        sheets = df_excel.sheet_names
        ui.Select_Tags.clear()
        ui.Select_Tags.addItems(sheets)

if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    ui = Main()
    ui.setupUi(MainWindow)
    ui.Initiative_Select.addItems(["Victoria"])
    ui.Line_Select.addItems(["badboihtr109.bcc.pg.com/api/datasources/proxy/1"])
    utils = Utils(ui)
    ui.pushButton.clicked.connect(utils.fetch_all_parameters)
    ui.Choose_File.clicked.connect(utils.populate_parameter_groups)
    ui.pushButton_analysis.clicked.connect(utils.show_images)
    ui.next_image.clicked.connect(utils.next_image)
    ui.previous_image.clicked.connect(utils.previous_image)

    MainWindow.show()
    sys.exit(app.exec_())