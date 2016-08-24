 {"configuration":{
 "name":"group_name",
 "tables":
    [
    <#list tableids as tableid>
     {
     	"$type": "oid",
     	"$value":"${tableid}"
     } <#sep>, </#sep>
     </#list>
    ],
 
     "scheduling":"none",
     "combined_schedule":{},
     "cdc_schedule":{},
     "merge_schedule":{},
     "fullload_schedule":{},
     "data_expiry_schedule":{
            "schedule_status":"disabled"
      }
}}
