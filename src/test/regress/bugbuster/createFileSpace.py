import os
import socket
import string
if __name__ == "__main__":
	tokens= []
	with open("bugbuster/data/countpri.txt") as prif:
		data = prif.read()
		tokens = string.split(data, '\n')
		
	
	pri_count=int(tokens[-4].strip())
	print "pri_count"+ str(pri_count)
	with open("bugbuster/data/countmir.txt") as prim:
		data = prim.read()
		tokens = string.split(data, '\n')

	mir_count=int(tokens[-4].strip())
	print "mir_count"+ str(mir_count) 
	i=1

	cd = os.getcwd();
	#create a config file
	f = open("bugbuster/bbfs_config",'w');
	f.write("filespace:bbfs2\n")
	hn=socket.gethostname()
	while i <=pri_count:
		if (i==1):
			f.write(hn+":"+str(i)+":"+cd+"/bugbuster/bbfs/master/demoDataDir-m\n")
			i+=1
			os.makedirs("bugbuster/bbfs/master");
			continue
		os.makedirs("bugbuster/bbfs/seg"+str(i-1));
		f.write(hn+":"+str(i)+":"+cd+"/bugbuster/bbfs/seg"+str(i-1)+"/demoDataDir-"+str(i-2)+"\n")
		i+=1
	j =i
	i=1
	while i <=mir_count:
		f.write(hn+":"+str(j)+":"+cd+"/bugbuster/bbfs/mir"+str(i-1)+"/demoDataDir-"+str(i-1)+"\n")
		os.makedirs("bugbuster/bbfs/mir"+str(i-1));
		i+=1
		j+=1
				
