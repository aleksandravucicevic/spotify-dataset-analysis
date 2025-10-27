# Analiza Spotify podataka korištenje PySpark-a
Ovaj projektni zadatak demonstrira upotrebu Apache Spark-a (PySpark) okruženja za analizu velikog skupa muzičkih podataka sa platforme Spotify.
Kroz seriju analiza istražuju se veze između popularnosti pjesama, muzičkih karakteristika (energija, valenca, tempo) i metapodataka (izvođač, žanr, album).

# Pregled projekta
Cilj projekta je da se prikaže kako se Spark DataFrame API može koristiti za:
* obradu i čišćenje velikih CSV fajlova
* izvođenje složenih statističkih analiza nad velikim skupovima podataka
* kombinovanje i poređenje metapodataka i muzičkih karakteristika
* serijalizaciju rezultata u JSON format.

Podaci se automatski učitavaju, obrađuju i analiziraju kroz 8 definisanih funkcija, a rezultati se čuvaju u posebne JSON fajlove.

# Glavne analize
1. 10 najčešćih kolaboracija izvođača - analizira najčešće saradnje između izvođača, njihovu popularnost, kao i solo popularnost svakog od izvođača iz kolaboracija, te poredi popularnost kolaboracije i popularnost najmanje popularnog izvođača koji je učestvovao u kolaboraciji.
2. Breakthrouh pjesme - identifikuje pjesme sa izuzetno visokom popularnošću sa inače manje popularnih albuma i analizira njihove audio karakteristike.
3. Tempo "sweet spot" - istražuje optimalne vrijednosti tempa za 5 najpopularnijih žanrova.
4. Veza između eksplicitnog teksta i popularnosti - mjeri korelaciju između eksiplicitnosti i popularnosti pjesama po žanru.
5. Trajanje i popularnost - upoređuje popularnost 10 najdužih pjesama sa visokom stopom plesivosti (danceability) sa prosječnom popularnošću pjesama u istom žanru kako bi se utvrdilo da li duže trajanje utiče na uspjeh.
6. Valenca i eksplicitnost - analizira koliko su eksplicitne pjesme "pozitivnije" u smislu valence i da li je taj odnos dosljedan na svim nivoima popularnosti.
7. Raznolikost žanrova po izvođačima - za umjetnike s najdosljednijom popularnošću (najniža standardna devijacija), analizira njihovu žanrovsku raznolikost, kako bi se utvrdilo da li je dosljednost povezana sa specijalizacijom za određene žanrove.
8. Instrumentalne i vokalne pjesme - poredi prosječnu popularnost žanrova sa visokim akustičnim i instrumentalnim vrijednostima i vokalno teških žanrovima, kako bi se utvrdilo da li instrumentalni fokus utiče na popularnost.

## Zavisnosti
Neophodno je imati instaliran Python 3.8+.  

Komanda za instalaciju PySpark-a:  
**Za Linux/MacOS**:  
* pip install pyspark  

**Za Windows**:  
neophodno je instalirati WSL (Windows Subsystem for Linux):  
_u PowerShell terminalu_:
  * wsl --install

_u WSL terminalu_:
* sudo apt-get update
* sudo apt-get install wget ca-certificates openjdk-21-jdk

Nakon instalacije WSL-a, potrebno je instalirati ekstenzije **Remote Development Pack** u  Visual Studio Code-u i pokrenuti komandu
* code .

koja će instalirati **Visual Studio Code Server** i otvoriti Visual Studio Code u Windows okruženju i povezati ga na server instaliran u okviru
WSL-a.

Prilikom rada na Visual Studio Code-u preko servera potrebno je ponovo instalirati ekstenzije koje želimo da koristimo - što je u ovom slučaju **Python Extension Pack**.

## Pokretanje
(_Opcionalno, ali preporučljivo_) Kreirati i aktivirati virtuelno okruženje:
* u okviru VSCode-a koji je povezan na server u okviru WSL-a otvoriti **Command Palette** (Ctrl+Shift+P) i ukucati i izabrati
  **Python: Create Environment**
* kao environemt type izabrati **venv** opciju
* za interpeter path izabrati Python iz **/usr/bin** foldera

Nakon toga otvoriti novi terminal i pokrenuti:
* pip install pyspark 

Komanda za instalaciju zavisnosti:
* pip install -r requirements.txt

Nakon toga je moguće pokrenuti Python skriptu:
* python program.py

# Autorska prava
© 2025 Aleksandra Vučićević
