import pandas as pd
from datetime import timedelta

def sessions_python():
    sessions = open("Sessions.csv", "r")

    row_sessions= sessions.read().split('\n')

    list_sessions=[]
    for r in row_sessions:
        list_sessions.append(r.split(','))


    dict_sessions= dict( )
    for l in list_sessions:
        k= l[0]+'-'+l[1]
        v=l[2]
        dict_sessions[k]=v

    ordered_dict_sessions= {k:v for k,v in sorted(dict_sessions.items(), key=lambda item:item[0])}

    list_sessions_order = []
    for k,v in ordered_dict_sessions.items():
        list_sessions_order.append([k,v])

    print(list_sessions_order)

    durations=[]
    counter_sessions=0
    for i in range(0,len(list_sessions)-1):
        if ((list_sessions_order[i+1][0].split('-')[0] == list_sessions_order[i][0].split('-')[0]) & ((list_sessions_order[i+1][1]=='Close') & (list_sessions_order[i][1]=='Open'))):
            durations.append(int(list_sessions_order[i+1][0].split('-')[1]) - int(list_sessions_order[i][0].split('-')[1]))
            counter_sessions+=1
            
    print(durations)
    print(sum(durations)/counter_sessions)


def sessions_pandas():
    # define treshold value
    T = timedelta(seconds=300)

    sessions= pd.read_csv('Sessions.csv', names=['User','Time','Status'])
    
    sessions= sessions.sort_values(by=['User','Time'])
    sessions['Time']= pd.to_numeric(sessions.Time, downcast='integer')
    sessions.loc[(sessions.User == sessions.User.shift(1)) & (sessions.Status=='Close') & (sessions.Status.shift(1)=='Open'),'duration'] = (sessions.Time - sessions.Time.shift(1))
    print(sessions)
    session_mean= sessions.duration.mean()
    print(session_mean)


if __name__ == "__main__":
    sessions_python()
    sessions_pandas()
