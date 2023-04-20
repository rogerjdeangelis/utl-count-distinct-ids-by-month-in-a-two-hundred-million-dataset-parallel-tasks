%let pgm=utl-count-distinct-ids-by-month-in-a-two-hundred-million-dataset-parallel-tasks;

Count distinct ids by month in a 192 million records parallel tasks;

1. SAS single HASH          6:43.85
2  WPS single HASH          6:20.18
3. SAS multi-task           1:33.25  (even though CPU throttled??)

Marks very fast ordered hash

Mark Keintz
mkeintz@outlook.com

1. Mark SAS HASH          2:49.02
2. Mark WPS HASH          2:05.06 (no cpu or storage limitations?)
3. Mark SAS multi task    0:47.06 (ist oartition to 47sec last took 15 sec)
                                  (should set up SPDE for parallel reads)
                                  (parallel index might eliminate patitioning)


I was able to process
192 million observations in a little in about 7 minutes and
a little less than 3 using Marks ordered hash, see above.
Between Microsoft and SAS cpu limits(2 of my 8 logical processors on my laptop),
my cpu utilization was capped at 25%. Will run on my beast computer when I get back to AZ.

Partitioning seems to help and it is a good idea to keep the partions for
further procrssing. I expect SPDE muti-task to be closer to 15 seconds.
Reads are not done in parallel except for SPDE? There were a lot of read conflicts.

github
https://tinyurl.com/2muaj8rx
https://github.com/rogerjdeangelis/utl-count-distinct-ids-by-month-in-a-two-hundred-million-dataset-parallel-tasks

StackOverflow
https://tinyurl.com/4zf55vhm
https://stackoverflow.com/questions/75377181/count-distinct-count-over-a-large-sas-dataset

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

%utlnopts; /*--- turn off macro messaging ----*/
%array(_mth,values=JAN FEB MAR APR MAY JUN JUL AUG);

libname sd1 "d:/sd1";

data sd1.have ;

    length date $3.;

    %do_over(_mth,phrase=%str(
              /*----  date="01?2022"d; ----*/
              switch=uniform(78651);
              date = "?";
              do i=1 to 24000000;
                 id=int(10000*uniform(4321))+1;
                 if id < 5000 and switch<.8 then id=1100;
                 if id < 9000 and date =: 'J' then id=1300;
                 if id < 9000 and date =: 'J' then id=1300;
                 if id < 9300 and date =: 'A' then id=1500;

                 output;
              end;
           ));
    drop i switch;

run;quit;
%utlopts; /*--- turn o nmacro messaging ----*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from HAVE total obs=192,000,000 18APR2023:16:08:39                                                         */
/*                                                                                                                        */
/*      Obs    DATE     ID                                                                                                */
/*                                                                                                                        */
/*        1    JAN     1300                                                                                               */
/*        2    JAN     1300                                                                                               */
/*        3    JAN     9629                                                                                               */
/*        4    JAN     9644                                                                                               */
/*        5    JAN     1300                                                                                               */
/*        6    JAN     1300                                                                                               */
/*        7    JAN     1300                                                                                               */
/*        8    JAN     1300                                                                                               */
/*        9    JAN     1300                                                                                               */
/*                                                                                                                        */
/**************************************************************************************************************************/
/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/
/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from WANT_SAS_HASH total obs=8 18APR2023:16:11:05                                                         */
/*                                                                                                                        */
/* Obs    DATE    UNIQUES                                                                                                 */
/*                                                                                                                        */
/*  1     APR        702                                                                                                  */
/*  2     JUL       1002                                                                                                  */
/*  3     JUN       1002                                                                                                  */
/*  4     FEB       5002                                                                                                  */
/*  5     JAN       1002                                                                                                  */
/*  6     AUG        702                                                                                                  */
/*  7     MAR       5002                                                                                                  */
/*  8     MAY       5002                                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*               _               _
 ___  __ _ ___  | |__   __ _ ___| |__
/ __|/ _` / __| | `_ \ / _` / __| `_ \
\__ \ (_| \__ \ | | | | (_| \__ \ | | |
|___/\__,_|___/ |_| |_|\__,_|___/_| |_|
*/
data _null_;
   dcl hash h ();
   h.definekey ("date");
   h.definedata ("date", "Uniques");
   h.definedone ();

   dcl hash u ();
   u.definekey ("date","id");
   u.definedone ();

   do until (dne);
      set sd1.have end = dne;
      if h.find() ne 0 then call missing (Uniques);
      if u.check() ne 0 then do;
         Uniques = sum (Uniques, 1);
         u.add();
      end;
      h.replace();
   end;

   h.output (dataset: "want_sas_hash");
   stop;
run;

/*               _
 ___  __ _ ___  | | ___   __ _
/ __|/ _` / __| | |/ _ \ / _` |
\__ \ (_| \__ \ | | (_) | (_| |
|___/\__,_|___/ |_|\___/ \__, |
                         |___/
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* 822   data _null_;                                                                                                     */
/* 823      dcl hash h ();                                                                                                */
/* 824      h.definekey ("date");                                                                                         */
/* 825      h.definedata ("date", "Uniques");                                                                             */
/* 826      h.definedone ();                                                                                              */
/* 827      dcl hash u ();                                                                                                */
/* 828      u.definekey ("date","id");                                                                                    */
/* 829      u.definedone ();                                                                                              */
/* 830      do until (dne);                                                                                               */
/* 831         set sd1.have end = dne;                                                                                    */
/* 832         if h.find() ne 0 then call missing (Uniques);                                                              */
/* 833         if u.check() ne 0 then do;                                                                                 */
/* 834            Uniques = sum (Uniques, 1);                                                                             */
/* 835            u.add();                                                                                                */
/* 836         end;                                                                                                       */
/* 837         h.replace();                                                                                               */
/* 838      end;                                                                                                          */
/* 839      h.output (dataset: "want_sas_hash");                                                                          */
/* 840      stop;                                                                                                         */
/* 841   run;                                                                                                             */
/*                                                                                                                        */
/* NOTE: The data set WORK.WANT_SAS_HASH has 8 observations and 2 variables.                                              */
/* NOTE: There were 192000000 observations read from the data set SD1.HAVE.                                               */
/* NOTE: DATA statement used (Total process time):                                                                        */
/*       real time           6:43.85                                                                                      */
/*       user cpu time       6:39.03                                                                                      */
/*       system cpu time     4.25 seconds                                                                                 */
/*       memory              2882.14k                                                                                     */
/*       OS Memory           18708.00k                                                                                    */
/*       Timestamp           04/18/2023 09:01:35 PM                                                                       */
/*       Step Count                        52  Switch Count  19                                                           */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                    _               _
__      ___ __  ___  | |__   __ _ ___| |__
\ \ /\ / / `_ \/ __| | `_ \ / _` / __| `_ \
 \ V  V /| |_) \__ \ | | | | (_| \__ \ | | |
  \_/\_/ | .__/|___/ |_| |_|\__,_|___/_| |_|
         |_|
*/

%utl_submit_wps64('
libname sd1 "d:/sd1";
data _null_;
   dcl hash h ();
   h.definekey ("date");
   h.definedata ("date", "Uniques");
   h.definedone ();
   dcl hash u ();
   u.definekey ("date","id");
   u.definedone ();
   do until (dne);
      set sd1.have end = dne;
      if h.find() ne 0 then call missing (Uniques);
      if u.check() ne 0 then do;
         Uniques = sum (Uniques, 1);
         u.add();
      end;
      h.replace();
   end;
   h.output (dataset: "sd1.want_wps_hash");
   stop;
run;
');

/*                               _ _   _       _            _
 ___  __ _ ___   _ __ ___  _   _| | |_(_)     | |_ __ _ ___| | __
/ __|/ _` / __| | `_ ` _ \| | | | | __| |_____| __/ _` / __| |/ /
\__ \ (_| \__ \ | | | | | | |_| | | |_| |_____| || (_| \__ \   <
|___/\__,_|___/ |_| |_| |_|\__,_|_|\__|_|      \__\__,_|___/_|\_\
*/
%utlnopts; /*--- turn off macro messaging ----*/
%array(_mth,values=JAN FEB MAR APR MAY JUN JUL AUG);

/*---- partition ----*/
data
  sd1.JAN
  sd1.FEB
  sd1.MAR
  sd1.APR
  sd1.MAY
  sd1.JUN
  sd1.JUL
  sd1.AUG
  ;

set sd1.have;
  select(date);
    when ( "JAN" ) output sd1.JAN ;
    when ( "FEB" ) output sd1.FEB ;
    when ( "MAR" ) output sd1.MAR ;
    when ( "APR" ) output sd1.APR ;
    when ( "MAY" ) output sd1.MAY ;
    when ( "JUN" ) output sd1.JUN ;
    when ( "JUL" ) output sd1.JUL ;
    when ( "AUG" ) output sd1.AUG ;
  end;
run;quit;

* SAVE the program in autocall library  c:/oto;
data _null_;file "c:\oto\_month.sas" lrecl=512;input;put _infile_;putlog _infile_;
cards4;
%macro _month(_month);
libname sd1 "d:/sd1";
data _null_;
   dcl hash h ();
   h.definekey ("date");
   h.definedata ("date", "Uniques");
   h.definedone ();

   dcl hash u ();
   u.definekey ("date","id");
   u.definedone ();

   do until (dne);
      set sd1.&_month(where=(date="&_month")) end = dne;
      if h.find() ne 0 then call missing (Uniques);
      if u.check() ne 0 then do;
         Uniques = sum (Uniques, 1);
         u.add();
      end;
      h.replace();
   end;

   h.output (dataset: "sd1.z&_month");
   stop;
run;
%mend _month;
;;;;
run;quit;

* test the macro interactively;
* note you can highlight and hit RMB(submit) to compile macro in your interactive session
  then highlight and RMB the code below to test;

%_month(JAN);

%let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin
c:\nul -sasautos c:\oto -autoexec c:\oto\Tut_Oto.sas
-work d:\wrk)) -nosplash;

* The argument of getmode is the remainder after dividing by 8;

options noxwait noxsync;
%let tym=%sysfunc(time());
systask kill sys1 sys2 sys3 sys4  sys5 sys6 sys7 sys8;
systask command "&_s -termstmt %nrstr(%_month(JAN);) -log d:\log\a1.log" taskname=sys1;
systask command "&_s -termstmt %nrstr(%_month(FEB);) -log d:\log\a2.log" taskname=sys2;
systask command "&_s -termstmt %nrstr(%_month(MAR);) -log d:\log\a3.log" taskname=sys3;
systask command "&_s -termstmt %nrstr(%_month(APR);) -log d:\log\a4.log" taskname=sys4;
systask command "&_s -termstmt %nrstr(%_month(MAY);) -log d:\log\a4.log" taskname=sys5;
systask command "&_s -termstmt %nrstr(%_month(JUN);) -log d:\log\a6.log" taskname=sys6;
systask command "&_s -termstmt %nrstr(%_month(JUL);) -log d:\log\a7.log" taskname=sys7;
systask command "&_s -termstmt %nrstr(%_month(AUG);) -log d:\log\a8.log" taskname=sys8;
waitfor sys1 sys2 sys3 sys4  sys5 sys6 sys7 sys8;
%put %sysevalf( %sysfunc(time()) - &tym);?

data humptyback;
  set
     sd1.zJAN
     sd1.zFEB
     sd1.zMAR
     sd1.zAPR
     sd1.zMAY
     sd1.zJUN
     sd1.zJUL
     sd1.zAUG
   ;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  NOTE: DATA statement used (Total process time):                                                                       */
/*        real time           0.04 seconds                                                                                */
/*        user cpu time       0.03 seconds                                                                                */
/*                                                                                                                        */
/*                                                                                                                        */
/*  Up to 40 obs from HUMPTYBACK total obs=8 19APR2023:07:46:08                                                           */
/*                                                                                                                        */
/*  Obs    DATE    UNIQUES                                                                                                */
/*                                                                                                                        */
/*   1     JAN       1002                                                                                                 */
/*   2     FEB       5002                                                                                                 */
/*   3     MAR      10000                                                                                                 */
/*   4     APR        702                                                                                                 */
/*   5     MAY      10000                                                                                                 */
/*   6     JUN       1002                                                                                                 */
/*   7     JUL       1002                                                                                                 */
/*   8     AUG        702                                                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*_  __            _                      _               _
|  \/  | __ _ _ __| | __  ___  __ _ ___  | |__   __ _ ___| |__
| |\/| |/ _` | `__| |/ / / __|/ _` / __| | `_ \ / _` / __| `_ \
| |  | | (_| | |  |   <  \__ \ (_| \__ \ | | | | (_| \__ \ | | |
|_|  |_|\__,_|_|  |_|\_\ |___/\__,_|___/ |_| |_|\__,_|___/_| |_|

*/

data want_mark (keep=lagdate uniques  rename=(lagdate=date));

  if 0 then set sd1.have ;

  declare hash hu (dataset:'sd1.have',ordered:'a');

    hu.definekey('date','id');
    hu.definedone();

  declare hiter i ('hu');

  do rc=i.first() by 0 until (i.next()^=0);

    lagdate=lag(date);
    if date^=lagdate then do;

      if uniques>0 then output;
      uniques=0;
    end;
    uniques+1;

  end;
  output;

run;

/*_  __            _                               _               _
|  \/  | __ _ _ __| | _____  __      ___ __  ___  | |__   __ _ ___| |__
| |\/| |/ _` | `__| |/ / __| \ \ /\ / / `_ \/ __| | `_ \ / _` / __| `_ \
| |  | | (_| | |  |   <\__ \  \ V  V /| |_) \__ \ | | | | (_| \__ \ | | |
|_|  |_|\__,_|_|  |_|\_\___/   \_/\_/ | .__/|___/ |_| |_|\__,_|___/_| |_|
                                      |_|
*/


%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64("

libname sd1 'd:/sd1';
libname wrk '&_pth';

data wrk.want_wps_mark (keep=lagdate uniques  rename=(lagdate=date));

  if 0 then set sd1.have ;

  declare hash hu (dataset:'sd1.have',ordered:'a');

    hu.definekey('date','id');
    hu.definedone();

  declare hiter i ('hu');

  do rc=i.first() by 0 until (i.next()^=0);

    lagdate=lag(date);
    if date^=lagdate then do;

      if uniques>0 then output;
      uniques=0;
    end;
    uniques+1;

  end;
  output;

run;
proc print;
run;quit;

");

/*                    _                      _ _   _       _               _
 _ __ ___   __ _ _ __| | __  _ __ ___  _   _| | |_(_)     | |__   __ _ ___| |__
| `_ ` _ \ / _` | `__| |/ / | `_ ` _ \| | | | | __| |_____| `_ \ / _` / __| `_ \
| | | | | | (_| | |  |   <  | | | | | | |_| | | |_| |_____| | | | (_| \__ \ | | |
|_| |_| |_|\__,_|_|  |_|\_\ |_| |_| |_|\__,_|_|\__|_|     |_| |_|\__,_|___/_| |_|
*/

/*---- partition ----*/
data
  sd1.JAN
  sd1.FEB
  sd1.MAR
  sd1.APR
  sd1.MAY
  sd1.JUN
  sd1.JUL
  sd1.AUG
  ;

set sd1.have;
  select(date);
    when ( "JAN" ) output sd1.JAN ;
    when ( "FEB" ) output sd1.FEB ;
    when ( "MAR" ) output sd1.MAR ;
    when ( "APR" ) output sd1.APR ;
    when ( "MAY" ) output sd1.MAY ;
    when ( "JUN" ) output sd1.JUN ;
    when ( "JUL" ) output sd1.JUL ;
    when ( "AUG" ) output sd1.AUG ;
  end;
run;quit;

* SAVE the program in autocall library  c:/oto;
data _null_;file "c:\oto\_month.sas" lrecl=512;input;put _infile_;putlog _infile_;
cards4;
%macro _month(_month);
libname sd1 "d:/sd1";
data sd1.z&_month (keep=lagdate uniques  rename=(lagdate=date));

  if 0 then set sd1.&_month ;

  declare hash hu (dataset:"sd1.&_month",ordered:"a");

    hu.definekey("date","id");
    hu.definedone();

  declare hiter i ("hu");

  do rc=i.first() by 0 until (i.next()^=0);

    lagdate=lag(date);
    if date^=lagdate then do;

      if uniques>0 then output;
      uniques=0;
    end;
    uniques+1;

  end;
  output;

run;
%mend _month;
;;;;
run;quit;

* test the macro interactively;
* note you can highlight and hit RMB(submit) to compile macro in your interactive session
  then highlight and RMB the code below to test;

%_month(JAN);

%let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin
c:\nul -sasautos c:\oto -autoexec c:\oto\Tut_Oto.sas
-work d:\wrk)) -nosplash;

* The argument of getmode is the remainder after dividing by 8;

options noxwait noxsync;
%let tym=%sysfunc(time());
systask kill sys1 sys2 sys3 sys4  sys5 sys6 sys7 sys8;
systask command "&_s -termstmt %nrstr(%_month(JAN);) -log d:\log\a1.log" taskname=sys1;
systask command "&_s -termstmt %nrstr(%_month(FEB);) -log d:\log\a2.log" taskname=sys2;
systask command "&_s -termstmt %nrstr(%_month(MAR);) -log d:\log\a3.log" taskname=sys3;
systask command "&_s -termstmt %nrstr(%_month(APR);) -log d:\log\a4.log" taskname=sys4;
systask command "&_s -termstmt %nrstr(%_month(MAY);) -log d:\log\a4.log" taskname=sys5;
systask command "&_s -termstmt %nrstr(%_month(JUN);) -log d:\log\a6.log" taskname=sys6;
systask command "&_s -termstmt %nrstr(%_month(JUL);) -log d:\log\a7.log" taskname=sys7;
systask command "&_s -termstmt %nrstr(%_month(AUG);) -log d:\log\a8.log" taskname=sys8;
waitfor sys1 sys2 sys3 sys4  sys5 sys6 sys7 sys8;
%put %sysevalf( %sysfunc(time()) - &tym);

data humptyback;
  set
     sd1.zJAN
     sd1.zFEB
     sd1.zMAR
     sd1.zAPR
     sd1.zMAY
     sd1.zJUN
     sd1.zJUL
     sd1.zAUG
   ;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  NOTE: DATA statement used (Total process time):                                                                       */
/*        real time           0.04 seconds                                                                                */
/*        user cpu time       0.03 seconds                                                                                */
/*                                                                                                                        */
/*                                                                                                                        */
/*  Up to 40 obs from HUMPTYBACK total obs=8 19APR2023:07:46:08                                                           */
/*                                                                                                                        */
/*  Obs    DATE    UNIQUES                                                                                                */
/*                                                                                                                        */
/*   1     JAN       1002                                                                                                 */
/*   2     FEB       5002                                                                                                 */
/*   3     MAR      10000                                                                                                 */
/*   4     APR        702                                                                                                 */
/*   5     MAY      10000                                                                                                 */
/*   6     JUN       1002                                                                                                 */
/*   7     JUL       1002                                                                                                 */
/*   8     AUG        702                                                                                                 */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|
*/
