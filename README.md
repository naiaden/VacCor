Processing the vacancies:
    The vacancies are stored in one XML file. Since processing this takes a lot of time, we want to do it distributedly. This script efficiently walks through the XML file, and separates it into individual vacancies, which are then passed on to multiple threads.

Processing the Twente News Corpus:
    Since ucto is a memory hogger for large input files, we tokenise the individual files. These are then concatenated into one large file. Kind of like the reverse process for the vacancies :-) This is because the background corpus is aggregated into type counts, ignoring any other information.

    
