from PyQt5.QtCore import QAbstractTableModel, Qt, pyqtSignal
from PyQt5.QtGui import QBrush, QColor

class Predicate:
    def __init__(self, item):
        self.item = item

    def __lt__(self, other):
        try:
            return self.item < other.item
        except TypeError:
            return False

class IndexTableModel(QAbstractTableModel):
    def __init__(self, data, parent=None):
        super(IndexTableModel, self).__init__(parent)
        self._data = data
        self.header_row = ['INDEX', 'OPEN', 'HIGH', 'LOW', 'CLOSE', "LTP", 'CHANGE', "TOTAL OI"]
        self.leftmost_header_name = 'Index'
        self.minimum_column_width = 20

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        try:
            return len(self._data[0]) if self.rowCount() else 0
        except:
            return 0

    def data(self, index, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            row = index.row()
            if 0 <= row < self.rowCount():
                column = index.column()
                if 0 <= column < self.columnCount():
                    return str(self._data[row][column])
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                if 0 <= section < len(self.header_row):
                    return self.header_row[section]

            elif orientation == Qt.Vertical:
                return None 
        
        return None
    
    def update_data(self, data):
        if self._data != data:
            self.beginResetModel()
            self._data = data
            self.endResetModel()


class OptionChainTableModel(QAbstractTableModel):
    dataChangedSignal = pyqtSignal(int)
    def __init__(self, data, parent=None):
        super(OptionChainTableModel, self).__init__(parent)
        self._data = data
        self.header_row = ["CE LOW","CE HIGH","CE OPEN",'CE VOL', 
                           'CE COI', 'CE OI', 'CE CHNG', 'CE LTP', 'STRIKES', 
                           'PE LTP', 'PE CHNG', 'PE OI', 'PE COI', 'PE VOL', 
                           "PE OPEN","PE HIGH","PE LOW"]
        self.highlight_value = None
        self.strikes_index = self.header_row.index('STRIKES')

        self.highlight_columns = {
            self.header_row.index('CE OI'), self.header_row.index('CE COI'),
            self.header_row.index('PE OI'), self.header_row.index('PE COI'),
            }
        
        self.ce_ltp_index = self.header_row.index('CE LTP')
        self.pe_ltp_index = self.header_row.index('PE LTP')
        
        self.ltp_columns = {
            self.ce_ltp_index, self.pe_ltp_index
            }
        
    def rowCount(self, parent=None):
        return len(self._data)
    
    def columnCount(self, parent=None):
        try:
            return len(self.header_row) if self.rowCount() else 0
        except:
            return 0
   
    def convert_to_float(self, value):
        try:
            return float(value)
        except ValueError:
            return None
        
    def convert_to_int(self, value):
        if value is not None:
            try:
                return int(value)
            except ValueError:
                return 0
        else:
            return 0
        
    def set_highlight_value(self, value):
        self.highlight_value = value
        top_left = self.index(0, 0)
        bottom_right = self.index(self.rowCount() - 1, self.columnCount() - 1)
        self.dataChanged.emit(top_left, bottom_right)
    
    def data(self, index, role=Qt.DisplayRole):
        row = index.row()
        column = index.column()

        if role == Qt.DisplayRole:
            return str(self._data[row][column])

        if role == Qt.BackgroundRole:
            
            if column == self.strikes_index:
                return QBrush(QColor(173, 216, 230))# QBrush(QColor(173, 216, 230))  
            
            column_values = [self.convert_to_int(row_data[column]) for row_data in self._data if row_data[column] != '']
                       
            if self.should_highlight_row(row):
                if column in self.highlight_columns:
                    if column_values:
                        max_values = sorted(column_values, reverse=True)[:2]  
                        if self.convert_to_int(self._data[row][column]) in max_values:
                            rank = max_values.index(self.convert_to_int(self._data[row][column]))
                            color_value =  int(255 - (1- rank / 2) * 128) 
                            #return QBrush(QColor(185, color_value, 232)) 
                            return QBrush(QColor(color_value, 255, 216))
                        else:
                            return QBrush(QColor(Qt.yellow))
                elif column in self.ltp_columns:
                    ce_open = self.convert_to_float(self._data[row][self.header_row.index('CE OPEN')])
                    ce_high = self.convert_to_float(self._data[row][self.header_row.index('CE HIGH')])
                    ce_low = self.convert_to_float(self._data[row][self.header_row.index('CE LOW')])
                    pe_open = self.convert_to_float(self._data[row][self.header_row.index('PE OPEN')])
                    pe_high = self.convert_to_float(self._data[row][self.header_row.index('PE HIGH')])
                    pe_low = self.convert_to_float(self._data[row][self.header_row.index('PE LOW')])
                    ce_ltp = self.convert_to_float(self._data[row][self.header_row.index('CE LTP')])
                    pe_ltp = self.convert_to_float(self._data[row][self.header_row.index('PE LTP')])

                    if column == self.ce_ltp_index:
                        if ce_open == ce_high == ce_low:
                            return QBrush(QColor(Qt.yellow))                      
                        elif ce_open == ce_high and ce_ltp is not None :
                            return QBrush(QColor(255, 220, 220)) 
                        elif ce_open == ce_low and ce_ltp is not None:
                            return QBrush(QColor(220, 255, 220)) 
                        else:
                            return QBrush(QColor(Qt.yellow))

                    if column == self.pe_ltp_index:
                        if pe_open == pe_high == pe_low:
                            return QBrush(QColor(Qt.yellow))                        
                        elif pe_open == pe_high and pe_ltp is not None:
                            return QBrush(QColor(255, 220, 220)) 
                        elif pe_open == pe_low and pe_ltp is not None:
                            return QBrush(QColor(220, 255, 220)) 
                        else:
                            return QBrush(QColor(Qt.yellow))
                else:
                    return QBrush(QColor(Qt.yellow))
            elif column in self.highlight_columns:
                if column_values:
                    max_values = sorted(column_values, reverse=True)[:2]  
                    if self.convert_to_int(self._data[row][column]) in max_values:
                        rank = max_values.index(self.convert_to_int(self._data[row][column]))
                        color_value =  int(255 - (1- rank / 2) * 128) 
                        #return QBrush(QColor(185, color_value, 232)) 
                        return QBrush(QColor(color_value, 255, 216))
            elif column in self.ltp_columns:
                ce_open = self.convert_to_float(self._data[row][self.header_row.index('CE OPEN')])
                ce_high = self.convert_to_float(self._data[row][self.header_row.index('CE HIGH')])
                ce_low = self.convert_to_float(self._data[row][self.header_row.index('CE LOW')])
                pe_open = self.convert_to_float(self._data[row][self.header_row.index('PE OPEN')])
                pe_high = self.convert_to_float(self._data[row][self.header_row.index('PE HIGH')])
                pe_low = self.convert_to_float(self._data[row][self.header_row.index('PE LOW')])
                ce_ltp = self.convert_to_float(self._data[row][self.header_row.index('CE LTP')])
                pe_ltp = self.convert_to_float(self._data[row][self.header_row.index('PE LTP')])

                if column == self.ce_ltp_index:
                    if ce_open == ce_high == ce_low:
                        return                        
                    elif ce_open == ce_high and ce_ltp is not None :
                        return QBrush(QColor(255, 220, 220))  
                    elif ce_open == ce_low and ce_ltp is not None:
                        return QBrush(QColor(220, 255, 220)) 
                
                if column == self.pe_ltp_index:
                    if pe_open == pe_high == pe_low:
                        return                        
                    elif pe_open == pe_high and pe_ltp is not None:
                        return QBrush(QColor(255, 220, 220)) 
                    elif pe_open == pe_low and pe_ltp is not None:
                        return QBrush(QColor(220, 255, 220)) 
                       
        return None
    
    def should_highlight_row(self, row):
        strikes_column = self.header_row.index('STRIKES')
        value = self._data[row][strikes_column]
        return value == self.highlight_value   # highlight test
    
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                if 0 <= section < len(self.header_row):
                    return self.header_row[section]
        return None
    
    def update_data(self, data):
        if self._data != data:
            self.beginResetModel()
            self._data = data
            self.endResetModel()
           