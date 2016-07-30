#include<cstdio>

struct ScoreType{
	int userID,itemID,pref;
};

struct ScoreType score[1300000];

int main(int argc, char *argv[]){
	if(argc < 3) printf("Input Error!\n");
	
	int partN  = 0;
	int i = 0;
	while(argv[1][i] != '\0')
		partN = partN*10 + argv[1][i++] - '0';
	
	FILE *fin = fopen(argv[2],"r");
	int userID,itemID,pref;
	int scoreN = 0;

	while(fscanf(fin,"%d%d%d",&userID,&itemID,&pref) != EOF){
		score[scoreN].userID = userID;
		score[scoreN].itemID = itemID;
		score[scoreN++].pref = pref;
	}
	fclose(fin);

	char filenamebase[] = "Ouru0.base";
	char filenametest[] = "Ouru0.test";
	for(int  i = 0; i < partN; i++){
		if(i > 0){
			filenamebase[4]++;
			filenametest[4]++;
		}
		FILE *fbase = fopen(filenamebase,"w");
		FILE *ftest = fopen(filenametest,"w");
		
		int s = 0;
		int thisuser;
		while(s < scoreN){
			thisuser = score[s].userID;
			int r = s;
			while(r < scoreN && score[r].userID == thisuser) r++;
			int steplen = partN;
			int testindex = s + i;
			for(int t = s; t < r; t++){
				if(t == testindex){
					fprintf(ftest,"%d\t%d\t%d\n",score[t].userID, score[t].itemID, score[t].pref);
					testindex += steplen;
				}else{
					fprintf(fbase,"%d\t%d\t%d\n",score[t].userID, score[t].itemID, score[t].pref);
				}
			}
			s = r;
		}

		fclose(fbase);
		fclose(ftest);
	}
	return 0;
}
