import time
import pandas as pd
import numpy as np

CITY_DATA = { 'chicago': 'chicago.csv',
              'new york city': 'new_york_city.csv',
              'washington': 'washington.csv' }

def get_filters():
    """
    Asks user to specify a city, month, and day to analyze.

    Returns:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    """
    print('Hello! Let\'s explore some US bikeshare data!')
    # TO DO: get user input for city (chicago, new york city, washington). HINT: Use a while loop to handle invalid inputs
    city = ''
    while city not in CITY_DATA.keys():
        print('\nWelcome! \nPlease choose a city: \nChicago   New York City   Washington')
        city = input().lower()
        if city not in CITY_DATA.keys():
            print('Sorry, your entry is\'t in our system. \nPlease try again.')
    print(f'\nYou have choosen {city.title()}')

    # TO DO: get user input for month (all, january, february, ... , june)
    MONTH_DATA = {'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6, 'all': 7}
    month = ''
    while month not in MONTH_DATA.keys():
        print('\nPlease enter your desired month between January and June; \nIf you\'d like to view all months please enter \'All\'')
        month = input().lower()
        if month not in MONTH_DATA.keys():
            print('Sorry, your entry is\'t in our system. \nPlease try again.')
    print(f'\nYou have choosen {month.title()}')

    # TO DO: get user input for day of week (all, monday, tuesday, ... sunday)
    DAY_DATA = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'all']
    day = ''
    while day not in DAY_DATA:
        print('\nPlease enter your desired day of the week; \nIf you\'d like to view all days of the weeks please enter \'All\'')
        day = input().lower()
        if day not in DAY_DATA:
            print('Sorry, your entry is\'t in our system. \nPlease try again.')
    print(f'\nYou have choosen {day.title()}')

    print(f'\nYou have choosen to see the city {city.title()}, during {month.title()}, on {day.title()}')
    print('-'*40)
    return city, month, day


def load_data(city, month, day):
    """
    Loads data for the specified city and filters by month and day if applicable.

    Args:
        (str) city - name of the city to analyze
        (str) month - name of the month to filter by, or "all" to apply no month filter
        (str) day - name of the day of week to filter by, or "all" to apply no day filter
    Returns:
        df - Pandas DataFrame containing city data filtered by month and day
    """
    df = pd.read_csv(CITY_DATA[city])
    
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['month'] = df['Start Time'].dt.month
    df['day_of_week'] = df['Start Time'].dt.weekday 
    
    if month != 'all':
        months = ['january', 'february', 'march', 'april', 'may', 'june']
        month = months.index(month) + 1
        df = df[df['month'] == month]
    
    if day != 'all':
        df = df[df['day_of_week'] == day.title()]
        


    return df


def time_stats(df):
    """Displays statistics on the most frequent times of travel."""

    print('\nCalculating The Most Frequent Times of Travel...\n')
    start_time = time.time()

    # TO DO: display the most common month
    popular_month = df['month'].mode()[0]
    print(f'\nThe most popular month is {popular_month}')

    # TO DO: display the most common day of week
    popular_day = df['day_of_week'].mode()[0]
    print(f'\nThe most popular day: {popular_day}')

    # TO DO: display the most common start hour
    df['hour'] = df['Start Time'].dt.hour
    popular_hour = df['hour'].mode()[0]
    print(f'\nThe most popular starting hour: {popular_hour}')

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(df):
    """Displays statistics on the most popular stations and trip."""

    print('\nCalculating The Most Popular Stations and Trip...\n')
    start_time = time.time()

    # TO DO: display most commonly used start station
    common_start_station = df['Start Station'].mode()[0]
    print(f'\nThe most commonly used start station: {common_start_station}')

    # TO DO: display most commonly used end station
    common_end_station = df['End Station'].mode()[0]
    print(f'\nThe most commonly used end station: {common_end_station}')

    # TO DO: display most frequent combination of start station and end station trip
    df['Start To End'] = df['Start Station'].str.cat(df['End Station'], sep=' to ')
    combine = df['Start To End'].mode()[0]

    print(f"\nThe most frequent combination of trips are from {combine}.")


    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(df):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration...\n')
    start_time = time.time()

    # TO DO: display total travel time
    total_duration = df['Trip Duration'].sum()
    minute, second = divmod(total_duration, 60)
    hour, minute = divmod(minute, 60)
    print(f'\nThe total trip duration is {hour} hours, {minute} minutes and {second} seconds.')
   
   
   # TO DO: display mean travel time
    average_duration = round(df['Trip Duration'].mean())
    mins, sec = divmod(average_duration, 60)
    if mins > 60:
        hrs, mins = divmod(mins, 60)
        print(f'\nThe average trip duration is {hrs} hours, {mins} minutes and {sec} seconds.')
    else:
        print(f'\nThe average trip duration is {mins} minutes and {sec} seconds.')


    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(df):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Stats...\n')
    start_time = time.time()

    # TO DO: Display counts of user types
    user_type = df['User Type'].value_counts()
    print(f'\nThe types of users by number:\n{user_type}')

    # TO DO: Display counts of gender
    try:
        gender = df['Gender'].value_counts()
        print(f'\nThe types of users by gender are:\n{gender}')
    except:
        print('\nThere is no Gender column.')

    # TO DO: Display earliest, most recent, and most common year of birth
    try:
        earliest = int(df['Birth Year'].min())
        recent = int(df['Birth Year'].max())
        common_year = int(df['Birth Year'].mode()[0])
        print(f'\nThe earliest year of birth is: {earliest}\nThe most recent year of birth is: {recent}\nThe most common year of birth is: {common_year}')
    except:
        print('There are no birth year details.')


    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)
    
    
def data_display(df):
    
    RESPONSE_LIST = ['yes', 'no']
    rawdata = ''
    counter = 0
    while rawdata not in RESPONSE_LIST:
        print('\nDo you wish to view the raw data?\nYes \nNo')
        rawdata = input().lower()
        if rawdata == 'yes':
            print(df.head())
        elif rawdata not in RESPONSE_LIST:
            print('\nPlease check your entry does not seem to be in the system.')

    while rawdata == 'yes':
        print('Do you wish to view more raw data?')
        counter += 5
        rawdata = input().lower()
        if rawdata == 'yes':
             print(df[counter:counter+5])
        elif rdata != 'yes':
             break

    print('-'*40)


def main():
    while True:
        city, month, day = get_filters()
        df = load_data(city, month, day)

        time_stats(df)
        station_stats(df)
        trip_duration_stats(df)
        user_stats(df)

        restart = input('\nWould you like to restart? Enter yes or no.\n')
        if restart.lower() != 'yes':
            break


if __name__ == "__main__":
	main()
