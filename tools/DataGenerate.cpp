#include<cstdio>
using namespace std;

int main(){
	FILE *fin = fopen("ratings.dat","r");
	FILE *fout = fopen("OurRatings.dat","w");
	int userID,itemID,pref;
	char *str = new char[110];
	while(fgets(str,100,fin)){
		userID = 0;
		itemID = 0;
		pref = 0;
		int i = 0;
		while(str[i] >= '0' && str[i] <= '9'){
			userID = userID * 10 + str[i] - '0';
			i++;
		}
		while(str[i] > '9' || str[i] < '0') i++;
		while(str[i] >= '0' && str[i] <= '9'){
			itemID = itemID * 10 + str[i] - '0';
			i++;
		}
		while(str[i] > '9' || str[i] < '0') i++;
		while(str[i] >= '0' && str[i] <= '9'){
			pref = pref * 10 + str[i] - '0';
			i++;
		}	
		if(userID > 11000) break;
		if(itemID <= 5094)
			fprintf(fout,"%d\t%d\t%d\n",userID,itemID,pref);
	}
	fclose(fin);
	fclose(fout);
	return 0;
}
