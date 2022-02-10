#include "mpi.h"
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <vector>
#define min(a,b) (((a) < (b)) ? (a) : (b))
using namespace std;

//functie cu rolul de a afisa topologia in consola pentru fiecare proces in parte
void afis(int rank, int matrix[10][10]) {

    cout << rank << " -> ";  
    for(int i = 0; i < 3; i++) {
        cout << i << ':';
        for(int j = 1; j <= matrix[i][0]; j++) {
            if(j < matrix[i][0]) {
                cout << matrix[i][j] << ',';
            } else {
                cout << matrix[i][j];
            }
        }
        cout << ' ';
    }
    cout << endl;
}


int main (int argc, char *argv[])
{
    int numtasks, rank;
    int number_processes;
    int rank_worker;
    //am utilizat o matrice pentru salvarea topologiei
    int matrix[10][10];

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    int recv_num;
    int randomNumber;
    int vector[10];
    int N;
    //vector cu valorile care urmeaza sa fie prelucrate in etapa 2 a temei
    int v[7000];
    int total_workers = 0;
    //in acest vector salvez ordinea fiecarui worker din topologie
    //cu rolul de a distribui calculele in mod echilibrat asupra vectorului cu valori de la etapa 2
    int positions[100];

    //mesajul care trebuie trimis procesului
    char* send_message = (char *)malloc(6 * sizeof(char));
    //mesajul primit de la proces
    char* recv_message = (char *)malloc(6 * sizeof(char));
    MPI_Status status;
    //string cu rolul de a salva numele fisierului
    char fileName[50];
    //lungimea mesajul care va fi transmis/primit
    int length;
    //parametru care ma ajuta sa fac distinctia dintre 'worker' si 'coordinator'
    bool coordinator = false;
    //dimensiunea vectorului
    int dimV = 10;
    int index;

    FILE *fp;
    //acest for este destinat coordonatorilor
    for(int i = 0; i < 3 && coordinator == false ; i++) {
        // in cazul in care rank-ul procesului se identifica cu cel al coordonatorului
        if(rank == i) {
            sprintf(fileName, "./../tests/test1/cluster%d.txt", rank); 
            //fiecare coordonator isi deschide fisierul cu toti workerii asignati lui
            //sprintf(fileName, "cluster%d.txt", rank); 
            fp = fopen(fileName, "r");

	        if (fp == NULL) {
	        	return 0;
            }

            //stochez fiecare worker in topologie, iar pe pozitia 0 de pe fiecare linie
            //se va afla numarul de procese worker pentru fiecare coordonator
            fscanf(fp, "%d", &number_processes);
            matrix[rank][0] = number_processes;

            //adaug procesele worker in matrice
            for(int j = 1; j <= number_processes; j++) {
                fscanf(fp, "%d", &rank_worker);
                matrix[rank][j] = rank_worker;
            }

            //coordonatorul curent va primi/trimite informatii despre topologia lui(ce procese worker are) de la/catre vecinii sai

            if(rank == 0 || rank == 1) {
                    //mai intai trimite mesajul 
                    sprintf(send_message, "M(%d,2)", rank); 
                    length = strlen(send_message);
                    //trimit lungimea mesajului
                    MPI_Send(&length, 1, MPI_INT, 2, 0, MPI_COMM_WORLD);   
                    MPI_Send(send_message, length, MPI_CHAR, 2, 0, MPI_COMM_WORLD);  

                    //apoi trimite informatia despre topologie
                    MPI_Send(matrix[rank], dimV, MPI_INT, 2, 0, MPI_COMM_WORLD);     

                    //primeste mesajul de receptionare
                    MPI_Recv(&length, 1, MPI_INT, 2, 0, MPI_COMM_WORLD, &status); 
                    MPI_Recv(recv_message, length, MPI_CHAR, 2, 0, MPI_COMM_WORLD, &status); 
                    cout << recv_message << endl;

                    // + topologia partiala de la coordonatorul vecin
                    MPI_Recv(matrix[2], dimV, MPI_INT, 2, 0, MPI_COMM_WORLD, &status);

                    MPI_Recv(&index, 1, MPI_INT, 2, 0, MPI_COMM_WORLD, &status);
                    MPI_Recv(matrix[index], dimV, MPI_INT, 2, 0, MPI_COMM_WORLD, &status);
            }

            if(rank == 2) {
                for(int j = 0; j <= 1; j++) {
                    //mai intai trimite mesajul 
                    sprintf(send_message, "M(%d,%d)", rank , j); 
                    length = strlen(send_message);
                    //trimit lungimea mesajului
                    MPI_Send(&length, 1, MPI_INT, j, 0, MPI_COMM_WORLD);   
                    MPI_Send(send_message, length, MPI_CHAR, j, 0, MPI_COMM_WORLD);  

                    //apoi trimite informatia despre topologie
                    MPI_Send(matrix[rank], dimV, MPI_INT, j, 0, MPI_COMM_WORLD);     

                    //primeste mesajul de receptionare
                    MPI_Recv(&length, 1, MPI_INT, j, 0, MPI_COMM_WORLD, &status); 
                    MPI_Recv(recv_message, length, MPI_CHAR, j, 0, MPI_COMM_WORLD, &status); 
                    cout << recv_message << endl;

                    // + topologia partiala de la coordonatorul vecin
                    MPI_Recv(matrix[j], dimV, MPI_INT, j, 0, MPI_COMM_WORLD, &status); 

                }

                index = 0;
                MPI_Send(&index, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);  
                MPI_Send(matrix[0], dimV, MPI_INT, 1, 0, MPI_COMM_WORLD);  

                index = 1;
                MPI_Send(&index, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);  
                MPI_Send(matrix[1], dimV, MPI_INT, 0, 0, MPI_COMM_WORLD);                   
                
            }

            //acesta mai departe trimite informatii despre topologie catre procesele de tip worker asignate lui
            for (int j = 1; j <= matrix[rank][0]; j++) {
                sprintf(send_message, "M(%d,%d)", rank, matrix[rank][j]); 
                //mai intai trimite mesajul 
                length = strlen(send_message);
                //trimit lungimea mesajului
                MPI_Send(&length, 1, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);  
                MPI_Send(send_message, length, MPI_CHAR, matrix[rank][j], 0, MPI_COMM_WORLD);   

                //matrix[i] reprezinta lista cu procese worker asignate coordonatorului i
                MPI_Send(matrix[0], dimV, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);  
                MPI_Send(matrix[1], dimV, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);  
                MPI_Send(matrix[2], dimV, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);  

                //la final va primi un mesaj de receptionare de la procesul worker
                //primesc lungimea mesajului
                MPI_Recv(&length,  1 , MPI_INT , matrix[rank][j] , 0 , MPI_COMM_WORLD , &status);
                MPI_Recv(recv_message,  length , MPI_CHAR , matrix[rank][j] , 0 , MPI_COMM_WORLD , &status);
                cout << recv_message << endl;
            }

            coordinator = true;
        }

    }

    //cazul in care rank-ul procesului nu este unul de tip coordonator
    if(coordinator == false) {

        //primeste mesajul de receptionare de la coordonatorul lui
        MPI_Recv(&length, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status); 
        MPI_Recv(recv_message, length, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status); 
        cout << recv_message << endl;

        //primeste informatiile despre procesele worker pentru fiecare coordonator
        //aceste informatii le primeste de la coordonatorul sau
        MPI_Recv( matrix[0],  dimV , MPI_INT , MPI_ANY_SOURCE , 0 , MPI_COMM_WORLD , &status);
        MPI_Recv( matrix[1],  dimV , MPI_INT , MPI_ANY_SOURCE , 0 , MPI_COMM_WORLD , &status);
        MPI_Recv( matrix[2],  dimV , MPI_INT , MPI_ANY_SOURCE , 0 , MPI_COMM_WORLD , &status);

        //trimite mai departe un mesaj ca le a primit
        sprintf(send_message, "M(%d,%d)", rank, status.MPI_SOURCE); 
        length = strlen(send_message);

        MPI_Send(&length, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);  
        MPI_Send(send_message,  length, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD);  

    }

    MPI_Barrier(MPI_COMM_WORLD);

    //afisez topologia 
    afis(rank, matrix);
    
    //calculez numarul total de procese worker
    total_workers = matrix[0][0] + matrix[1][0] + matrix[2][0];


    //mai departe in vectorul positions adaug pozitile tuturor
    //proceselor worker din intreaga topologie pentru asignarea in mod
    //egal a calculelor efectuate asupra vectorului
    int counter = 0;
    for(int i = 0; i < 3; i++) {
        for(int j = 1; j <= matrix[i][0]; j++) {
            positions[matrix[i][j]] = counter;
            counter++;
        }
    }


    MPI_Barrier(MPI_COMM_WORLD);

    //daca rank-ul procesului este 0
    if(rank == 0) {

        //citesc numarul de elemente
        N = atoi(argv[1]);

        //construiesc vectorul
        for(int i = 0; i < N; i++) {
            v[i] = i;
        }

        //trimit si primesc informatii catre / de la coordonatori legat de vectorul care urmeaza
        //sa fie prelucrat
        for(int j = 1; j < 3; j++) {
            MPI_Send(&N, 1, MPI_INT, j, 0, MPI_COMM_WORLD);     
            MPI_Send(v, N, MPI_INT, j, 0, MPI_COMM_WORLD);    

            MPI_Recv(v, N, MPI_INT, j, 0, MPI_COMM_WORLD, &status);
        }

        //trimit si primesc informatii catre /de la procesele worker asignate lui legat de vectorul care urmeaza
        //sa fie prelucrat
        for (int j = 1; j <= matrix[rank][0]; j++) {
            MPI_Send(&N, 1, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);     
            MPI_Send(v, N, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);  

            MPI_Recv(v, N, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD, &status);
        }

        //afisez vectorul prelucrat
        cout << "Rezultat: ";

        for(int i = 0 ; i < N; i++) {
            cout << v[i] << " ";
        }

        cout << endl;


    } else if(rank == 1 || rank == 2) {
        //ceilalti coordonatori vor primi vectorul de la coordonatorul 0
        MPI_Recv(&N, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        MPI_Recv(v, N, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);

        //acestia vor trimite mai departe vectorul catre procesele worker
        //si vor primi vectorul partial prelucrat de la fiecare proces worker 
        for (int j = 1; j <= matrix[rank][0]; j++) {
            MPI_Send(&N, 1, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);     
            MPI_Send(v, N, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD);  

            MPI_Recv(v, N, MPI_INT, matrix[rank][j], 0, MPI_COMM_WORLD, &status);
        }

        //la final se trimite vectorul modificat catre coordonatorul 0
        MPI_Send(v, N, MPI_INT, 0, 0, MPI_COMM_WORLD);

    } else {
        //in cazul in care rank-ul procesului este unul specific de worker
        //acesta va primi vectorul initial
        MPI_Recv(&N,  1 , MPI_INT , MPI_ANY_SOURCE , 0 , MPI_COMM_WORLD , &status);
        MPI_Recv(v,  N , MPI_INT , MPI_ANY_SOURCE , 0 , MPI_COMM_WORLD , &status);

        //pe baza vectorului positions se va determina portiunea din vector care va urma sa fie modificata
        int start = positions[rank] * (double)N / total_workers;
        int end = min( ((positions[rank] + 1) * (double)N / total_workers), N);

        //se realizeaza calculele necesare
        for(int i = start ; i < end; i++) {
            v[i] = v[i] * 2;
        }

        //la final se trimite vectorul cu portiunea modificata 
        MPI_Send(v, N, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);  
    }


    MPI_Finalize();

}
