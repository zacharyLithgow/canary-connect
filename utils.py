def median(data_points):
    # we consider that the data_points have already been sorted
    mid = int(len(data_points) / 2)
    if len(data_points) % 2 == 0:
        # even: need to get the average of the two mid numbers
        return (data_points[mid - 1] + data_points[mid]) / 2.0
    else:
        # odd: there is only one number
        return data_points[mid]
