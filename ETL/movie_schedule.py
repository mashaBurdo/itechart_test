from datetime import datetime, timedelta


class Movie:
    def __init__(self, date_intervals):
        self.date_intervals = date_intervals

    def schedule_gen(self):
        for index in range(len(self.date_intervals)):
            date = self.date_intervals[index][0]
            while date <= self.date_intervals[index][1]:
                yield date
                date += timedelta(days=1)

    def schedule(self):
        print(*list(self.schedule_gen()), sep="\n")


m = Movie(
    [
        (datetime(2020, 1, 1), datetime(2020, 1, 7)),
        (datetime(2020, 1, 15), datetime(2020, 2, 7)),
    ]
)
m.schedule()
