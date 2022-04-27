import xml.etree.ElementTree as ET
import datetime
dataset_file='big.osm'
fname = 'large.txt'
ref='00:00:00'
ref_time = datetime.datetime.strptime(ref,"%H:%M:%S")

def conv(timestamp):
	cur=timestamp[11:19]
	cur_time=datetime.datetime.strptime(cur,"%H:%M:%S")
	return (cur_time - ref_time).seconds

if __name__ == '__main__':

	lol=[]
	f=open(fname,'w')
	tree = ET.parse(dataset_file)
	root = tree.getroot()
	for way in root.findall('node'):
		timestamp,x,y=conv(way.attrib['timestamp']),int(float(way.attrib['lat'])*10000000),int(float(way.attrib['lon'])*10000000)
		f.write(str(timestamp)+' '+str(x)+' '+str(y)+'\n')
		lol.append((timestamp,x,y))

	for i in range(13):
		for j in range(0,len(lol)):
			timestamp,x,y=lol[j]
			f.write(str(timestamp)+' '+str(x)+' '+str(y)+'\n')
	f.close()