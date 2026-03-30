from wskazniki import adx__chat as adxcht
from wskazniki import mcad__chat as mcad_analyze_result_object
from wskazniki import ichi as ichi_result_object
import tools as tools
from wskazniki.adx__chat import adx_result_enum
from wskazniki.mcad__chat import mcad_result_enum
from wskazniki.ichi import ichi_crossover_price_kiusen_result_enum


class AnalysisResult:
    def __init__(self, adx_analyze_result_obj : adxcht.adx_analyze_result_object  , mcad_analyze_result_obj : mcad_analyze_result_object , ichi_analyze_result_obj_K : ichi_result_object , ichi_analyze_result_obj_S : ichi_result_object):
        self.adx_analyze_result_obj = adx_analyze_result_obj
        self.mcad_analyze_result_obj = mcad_analyze_result_obj
        self.ichi_analazy_result_object_K = ichi_analyze_result_obj_K
        self.ichi_analazy_result_object_S = ichi_analyze_result_obj_S

    def __repr__(self):
        return f"AnalysisResult(adx_analyze_result_obj={self.adx_analyze_result_obj}, mcad_analyze_result_obj={self.mcad_analyze_result_obj} , ichi_analyze_result_obj={self.ichi_analazy_result_object})"
    
    def get_time_and_result(self ,diff_time_result, result_K_result , result_S_result):
        times_K = []
        times_S = []
        diff_time = {}
        if result_S_result != None :
            for key , value in diff_time_result.items():
                if "_S_" in key:
                    diff_time[key] = value

        if result_K_result != None :
            for key , value in diff_time_result.items():
                if "_K_" in key:
                    diff_time[key] = value

        for key , value in diff_time.items():
            if  result_K_result != None :
                list_key = key.split("_K_")
                if len(list_key) > 1:
                   times_K.append(value)
        
        for key , value in diff_time.items():
            if  result_S_result != None :
                list_key = key.split("_S_")
                if len(list_key) > 1:
                   times_S.append(value)

        times_K_sort = self.sort_int_table(times_K)
        times_S_sort = self.sort_int_table(times_S)

        return times_K_sort , times_S_sort
    
    def sort_int_table(self, int_table):
        """
        Sorts a list of integers in increasing (ascending) order.

        Args:
            int_table (list of int): The list of integers to sort.

        Returns:
            list of int: The sorted list in increasing order.
        """
        return sorted(int_table)
    def get_result(self):
        result_K = {}
        result_S = {}

        if self.adx_analyze_result_obj[0] == None or self.mcad_analyze_result_obj == None:
            return result_K , result_S
        #KUPNO
        if self.adx_analyze_result_obj[0].result == adx_result_enum.Wzrost_przeciecie :
            result_K["adx"] = "BUY"
            result_K["adx_trend"] = self.adx_analyze_result_obj[1].name
        
                
        if self.mcad_analyze_result_obj.result == mcad_result_enum.Wzrost_przeciecie :
            result_K["mcad"] = "BUY"
        
        for i in range(len(self.ichi_analazy_result_object_K)):
            result_name = tools.split_string_by_comma(self.ichi_analazy_result_object_K[i])[0]
            result_time = tools.split_string_by_comma(self.ichi_analazy_result_object_K[i])[1]
            if result_name== "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory":
                result_K["ichi_cross_price_senokuspan"] = "BUY"
            if result_name == "ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory":
                result_K["ichi_cross_price_kiusen"] = "BUY"
            if result_name == "ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory":
                result_K["ichi_cross_tenkansen_kiusen"] = "BUY"
        
        #SPRZEDAŻ
        if self.adx_analyze_result_obj[0].result == adx_result_enum.Spadek_przeciecie :
            result_S["adx"] = "SELL"
            result_S["adx_trend"] = self.adx_analyze_result_obj[1].name

        if self.mcad_analyze_result_obj.result == mcad_result_enum.Spadek_przeciecie :
            result_S["mcad"] = "SELL"

        for i in range(len(self.ichi_analazy_result_object_S)):
            result_name = tools.split_string_by_comma(self.ichi_analazy_result_object_S[i])[0]
            result_time = tools.split_string_by_comma(self.ichi_analazy_result_object_S[i])[1]
            if result_name== "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu":
                result_S["ichi_cross_price_senokuspan"] = "SELL"
            if result_name == "ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu":
                result_S["ichi_cross_price_kiusen"] = "SELL"
            if result_name == "ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu":
                result_S["ichi_cross_tenkansen_kiusen"] = "SELL"
       
        return result_K , result_S
    
    def get_time_difference(self):
        result = {}
  
        if self.adx_analyze_result_obj[0] == None or self.mcad_analyze_result_obj == None:
             return result
        dt1=tools.int_to_datetime(self.adx_analyze_result_obj[0].time)
        dt2 = tools.int_to_datetime(self.mcad_analyze_result_obj.time)
        
        dtK=[]
        for i in range(len(self.ichi_analazy_result_object_K)):
            t =  tools.split_string_by_comma(self.ichi_analazy_result_object_K[i])[1]
            time = tools.int_to_datetime(t)
            n = tools.split_string_by_comma(self.ichi_analazy_result_object_K[i])[0]
            if n == "ichi_crossover_price_kiusen_result_enum.Przeciecie_do_gory" :
                dtK.append(time)
            else :
                dtK.append(-1)
            
            if n == "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_gory" :
                dtK.append(time)
            else :
                dtK.append(-1)
            if n == "ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_gory" :
                dtK.append(time)
            else :
                dtK.append(-1)



       # dt3K = tools.int_to_datetime(tools.split_string_by_comma(self.ichi_analazy_result_object_K.time_of_cross_price_kiusen))
      #  dt4K = tools.int_to_datetime(tools.split_string_by_comma(self.ichi_analazy_result_object_K.time_of_cross_price_senokuspan))
      #  dt5K = tools.int_to_datetime(tools.split_string_by_comma(self.ichi_analazy_result_object_K._time_of_cross_tenkansen_kiusen))

        dtS = []
        for i in range(len(self.ichi_analazy_result_object_S)):
            t= tools.split_string_by_comma(self.ichi_analazy_result_object_S[i])[1]
            time = tools.int_to_datetime(t)
            n = tools.split_string_by_comma(self.ichi_analazy_result_object_S[i])[0]
            if n == "ichi_crossover_price_kiusen_result_enum.Przeciecie_do_dolu" :
                dtS.append(time)
            else :
                dtS.append(-1)
            if n == "ichi_crossover_price_senokuspan_result_enum.Przeciecie_do_dolu" :
                dtS.append(time)
            else :
                dtS.append(-1)
            if n == "ichi_crossover_tenkansen_kiusen_result_enum.Przeciecie_do_dolu" :
                dtS.append(time)
            else :
                dtS.append(-1)

       # dt3S = tools.int_to_datetime(tools.split_string_by_comma(self.ichi_analazy_result_object_S.time_of_cross_price_kiusen))
      #  dt4S = tools.int_to_datetime(tools.split_string_by_comma(self.ichi_analazy_result_object_S.time_of_cross_price_senokuspan))
       # dt5S = tools.int_to_datetime(tools.split_string_by_comma(self.ichi_analazy_result_object_S._time_of_cross_tenkansen_kiusen))


        dt1_minute = dt1.minute + dt1.hour * 60
        dt2_minute = dt2.minute + dt2.hour * 60


        result["adx_mcad"] = self.get_time_diff(dt1_minute , dt2_minute)
        try:
            if dtK[0] == -1: 
                raise ValueError("dtK[0] is -1, indicating no valid time found for price kiusen K")
            result["ichi_cross_price_kiusen_K_adx"] = abs(self.get_time_diff(dt1_minute , dtK[0].minute + dtK[0].hour * 60))
        except:
            result["ichi_cross_price_kiusen_K_adx"] = 0
        try:
            if dtK[1] == -1: 
                raise ValueError("dtK[1] is -1, indicating no valid time found for price senokuspan K")
            result["ichi_cross_price_senokuspan_K_adx"] = abs(self.get_time_diff(dt1_minute , dtK[1].minute + dtK[1].hour * 60))
        except:
            result["ichi_cross_price_senokuspan_K_adx"] = 0
        try:
            if dtK[2] == -1: 
                raise ValueError("dtK[2] is -1, indicating no valid time found for price tenkansen K")
            result["ichi_cross_tenkansen_kiusen_K_adx"] = abs(self.get_time_diff(dt1_minute , dtK[2].minute + dtK[2].hour * 60))
        except:
            result["ichi_cross_tenkansen_kiusen_K_adx"] = 0
        try:  
            if dtS[0] == -1: 
                raise ValueError("dtS[0] is -1, indicating no valid time found for price kiusen S")  
            result["ichi_cross_price_kiusen_S_adx"] = abs(self.get_time_diff(dt1_minute , dtS[0].minute + dtS[0].hour * 60))
        except:
            result["ichi_cross_price_kiusen_S_adx"] = 0
        try:  
            if dtS[1] == -1: 
                raise ValueError("dtS[1] is -1, indicating no valid time found for price senokuspan S")
            result["ichi_cross_price_senokuspan_S_adx"] = abs(self.get_time_diff(dt1_minute , dtS[1].minute + dtS[1].hour * 60))
        except:
            result["ichi_cross_price_senokuspan_S_adx"] = 0
        try:   
            if dtS[2] == -1: 
                raise ValueError("dtS[2] is -1, indicating no valid time found for price tenkansen S") 
            result["ichi_cross_tenkansen_kiusen_S_adx"] = abs(self.get_time_diff(dt1_minute , dtS[2].minute + dtS[2].hour * 60))
        except:
            result["ichi_cross_tenkansen_kiusen_S_adx"] = 0
        try:
            if dtK[0] == -1: 
                raise ValueError("dtK[0] is -1, indicating no valid time found for price kiusen K")
            result["ichi_cross_price_kiusen_K_mcad"] = abs(self.get_time_diff(dt2_minute , dtK[0].minute + dtK[0].hour * 60))
        except:
            result["ichi_cross_price_kiusen_K_mcad"] = 0
        try:
            if dtK[1] == -1: 
                raise ValueError("dtK[1] is -1, indicating no valid time found for price senokuspan K")
            result["ichi_cross_price_senokuspan_K_mcad"] = abs(self.get_time_diff(dt2_minute , dtK[1].minute + dtK[1].hour * 60))
        except:
            result["ichi_cross_price_senokuspan_K_mcad"] = 0
        try:  
            if dtK[2] == -1: 
                raise ValueError("dtK[2] is -1, indicating no valid time found for price tenkansen K")  
            result["ichi_cross_tenkansen_kiusen_K_mcad"] = abs(self.get_time_diff(dt2_minute , dtK[2].minute + dtK[2].hour * 60))
        except:
            result["ichi_cross_tenkansen_kiusen_K_mcad"] = 0
        try: 
            if dtS[0] == -1: 
                raise ValueError("dtS[0] is -1, indicating no valid time found for price kiusen S")   
            result["ichi_cross_price_kiusen_S_mcad"] = abs(self.get_time_diff(dt2_minute , dtS[0].minute + dtS[0].hour * 60))
        except:
            result["ichi_cross_price_kiusen_S_mcad"] = 0
        try:  
            if dtS[1] == -1: 
                raise ValueError("dtS[1] is -1, indicating no valid time found for price senokuspan S")  
            result["ichi_cross_price_senokuspan_S_mcad"] = abs(self.get_time_diff(dt2_minute , dtS[1].minute + dtS[1].hour * 60))
        except:
            result["ichi_cross_price_senokuspan_S_mcad"] = 0
        try:    
            if dtS[2] == -1: 
                raise ValueError("dtS[2] is -1, indicating no valid time found for price tenkansen S") 
            result["ichi_cross_tenkansen_kiusen_S_mcad"] = abs(self.get_time_diff(dt2_minute , dtS[2].minute + dtS[2].hour * 60))
        except:
            result["ichi_cross_tenkansen_kiusen_S_mcad"] = 0


        return result
    
    def convert_time_to_minutes(self , time_diff):
        return time_diff / 60
    def get_time_diff(self,time1: int, time2: int) -> int:
        """
        Calculates the absolute difference between two times in minutes.

        Args:
            time1 (int): The first time in minutes.
            time2 (int): The second time in minutes.

        Returns:
            int: The absolute difference between the two times.
        """
        return abs(time1 - time2)