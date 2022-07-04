import psycopg2
import pymongo
import ssl
from bson.json_util import dumps, loads
import datetime
import collections
import json
from operator import truediv


# main function for embedding and Linking
def embedAndLinking(cursor, schema, tables, tNo, relax, pks, work, adj2, relations):
    
    # fetches data from postgres
    data=[]
    for t in tables:
        cursor.execute("""select * from \"{}\".\"{}\" ;""".format(schema,t))
        mykeys = [desc[0] for desc in cursor.description]
        myresults = cursor.fetchall()
        dicted_myresult = [dict(zip(mykeys, myresult)) for myresult in myresults]
        data.append(dicted_myresult)

    # embedding data
    for e in relax:
        data[e[0]] = embed2(data[e[0]], data[e[1]], pks[tables[e[0]]], pks[tables[e[1]]], data, tables[e[0]], tables[e[1]], relations)

    # print(pks)
    for i in range(len(work)) :
        for j in range(len(work)) :
            if (work[i][j] == 'l') : 
                f = False
                for ngh in adj2[tables[i]] :
                    if tNo[ngh] == j :
                        f = True
                        break
                if f == True :
                    work[i][j] = '-'

    for i in range(len(work)):
        for j in range(i):
            if(work[i][j] == 'l' and work[j][i] == 'l') :
                tb = -1
                for t in tables :
                    fi = False
                    fj = False
                    for ngh in adj2[t] :
                        if (tNo[ngh] == i) :
                            fi = True
                        if (tNo[ngh] == j) :
                            fj = True
                    if(fi == True and fj == True) :
                        tb = tNo[t]
                        break
                [data[i], data[j]] = linkManyToMany(data[i], data[j], data[tb], pks[tables[i]], pks[tables[j]], pks[tables[tb]], tables[i],tables[j], tables[tb], relations)
                # print("----------------")
                # print(data[i])
                # print("----------------")
                # print(data[j])
                # print("----------------")

    for i in range(len(work)) :
        for j in range(len(work)) :
            if(work[i][j] == 'l' and work[j][i] != 'l') :
                f = False
                for ngh in adj2[tables[i]] :
                    if tNo[ngh] == j :
                        f = True
                        break
                if f == False :
                    data[i] = link(data[i], data[j], pks[tables[i]], pks[tables[j]], tables[i],tables[j], relations)
    return data



# function for forward embedding
def embed2(t1, t2, pk1,pk2, data, t1Name, t2Name, relations):
    print("embedding "+ t2Name + " in " + t1Name)
    refersvia = []
    rel = relations[t2Name]
    for referring in rel :
        if referring[0] == t1Name :
            for key in referring[1] :
                refersvia.append(key)
            break

    # print(refersvia)
    # print(pk2)
    if(len(refersvia) == 0) :
        return embed1(t1, t2, pk1,pk2, data, t1Name, t2Name, relations)

    for tt in t1 :
        tt[t2Name] = []

    # f = True
    z= 0
    for t in t2 :
        z = z + 1
        
        for tt in t1 :
            flag = True
            for i in range( min(len(refersvia), len(pk2) )) :
                # if f == True :
                #     print(str(tt[refersvia[i]]) + " " + str(t[pk2[i]]))
                if(tt[refersvia[i]] != t[pk2[i]]) :
                    flag = False
            # f = False
            if(flag == True) :
                tt[t2Name].append(t) 
    # for t in t1 :
    #     order_id = [] 
    #     for pk in pk2 :
    #         order_id.append(t[pk])
    #     lst = []
    #     for lineItem in t2 :
    #         if lineItem[pk1] == order_id :
    #             lst.append(lineItem)
    #     t[t2Name] = lst
    
    # if(flag==1):
    #     for order in t2 :
    #         order_id = order[pk2]
    #         lst = []
    #         for lineItem in t11 :
    #             if lineItem[pk2] == order_id :
    #                 lst.append(lineItem)
    #         order[t1Name] = lst

    # print(json.dumps(t1, indent = 4))

    return t1



# function for reverse embedding
def embed1(t1, t2, pk1, pk2, data, t1Name, t2Name, relations):
    refersvia = []
    rel = relations[t1Name]
    for referring in rel :
        if referring[0] == t2Name :
            for key in referring[1] :
                refersvia.append(key)
            break

    for t in t1 :
        # order_id = t[pk1]
        order_id = []
        for pk in pk1 :
            order_id.append(t[pk])
        lst = []
        for lineItem in t2 :
            flag = True
            for i in range(min(len(refersvia),  len(order_id)) ) :
                if lineItem[refersvia[i]] != order_id[i] :
                    flag = False
            if flag == True :
                lst.append(lineItem)
            t[t2Name] = lst
    
    # if(flag==1):
    #     for order in t2 :
    #         order_id = order[pk2]
    #         lst = []
    #         for lineItem in t11 :
    #             if lineItem[pk2] == order_id :
    #                 lst.append(lineItem)
    #         order[t1Name] = lst

    # print(json.dumps(t1, indent = 4))

    return t1



# function for many to many linking
def linkManyToMany(data1, data2, datagum, pk1, pk2, pkgum, t1Name, t2Name, tbName, relations) :
    print("linking each other : " + t1Name + " <-> " + t2Name + " via " + tbName)  
    # print(pk1)
    rel1 = relations[t1Name]
    for referring in rel1 :
        if referring[0] == tbName :
            refersvia1 = referring[1]
            break
    rel2 = relations[t2Name]
    for referring in rel2 :
        if referring[0] == tbName :
            refersvia2 = referring[1]
            break
    # print(refersvia1)
    # print(refersvia2)
    for row in data1 :
        for key in pk2 :
            row[key]= [] # assume simple pk1
    for row in data2 :
        for key in pk1 :
            row[key] = [] # assume simple pk2

    for row in datagum :
        for row1 in data1 :
            f1 = True
            for i in range(len(refersvia1)) :
                if(row1[pk1[i]] != row[refersvia1[i]]) :
                    f1 = False 
                    break
            if f1 == True :
                for i in range(len(refersvia2)) :
                    row1[pk2[i]].append(row[refersvia2[i]])
        for row1 in data2 :
            f1 = True
            for i in range(len(refersvia2)) :
                if(row1[pk2[i]] != row[refersvia2[i]]) :
                    f1 = False 
                    break
            if f1 == True :
                for i in range(len(refersvia1)) :
                    row1[pk1[i]].append(row[refersvia1[i]])    
    return [data1, data2]
        


# function for single linking        
def link(data1, data2, pk1, pk2, t1Name, t2Name, relations) :    
    print("linking " + t2Name + " in " + t1Name)

    
    rel = relations[t1Name]
    for referring in rel :
        if referring[0] == t2Name :
            refersvia = referring[1][0]
            break

    indata1 = {}
    for row in data1 :
        indata1[row[pk1[0]]] = []

    for row in data2 :
        indata1[row[refersvia]].append(row[pk2[0]])

    for row in data1 :
        row[pk2[0]] = indata1[row[pk1[0]]]
    
    return data1
    # for order in data2 :
    #     for cust in data1 :
    #         if cust[pk1] == order[pk1] :
    #             if pk2 in cust :
    #                 cust[pk2].append(order[pk2])
    #             else :
    #                 cust[pk2] = []
    #                 cust[pk2].append(order[pk2])