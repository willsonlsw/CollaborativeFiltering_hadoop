#include<cstdio>

int main(){
	printf("Please input the N for Top-N recommedation:\n");
	int n;
	scanf("%d",&n);
	printf("Please input the Path of input data:\n");
	char *filepath = new char[100];
	scanf("%s",filepath);
	FILE *fin = fopen(filepath,"r");
	FILE *fout = fopen("Top-N.txt","w");
	
	char str[400000];
	char outstr[400000];
	while(fgets(str,400000,fin)){
		int k = 0;
		int i = 0,j = 0;
		while(str[i]!='\0' && k < n){
			while(str[i]!='\0' && str[i] != ':')
				outstr[j++] = str[i++];
			outstr[j++] = ' ';
			i++;
			k++;
			while(str[i]!='\0' && str[i] != ',')
				i++;
			i++;
		}
		outstr[j] = '\0';
		fprintf(fout,"%s\n",outstr);
	}
	fclose(fin);
	fclose(fout);
}