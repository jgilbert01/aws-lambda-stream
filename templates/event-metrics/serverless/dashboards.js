module.exports.events = JSON.stringify({
  widgets: [
    {
      type: 'metric',
      x: 0,
      y: 0,
      width: 24,
      height: 6,
      properties: {
        metrics: [
          [{ expression: "SEARCH(' MetricName=\"domain.event\" ', 'Sum', 300)", label: 'Events: ${SUM} : ${LABEL}', id: 'events' }],
        ],
        view: 'timeSeries',
        stacked: false,
        region: '${opt:region}',
        start: '-P1D',
        end: 'P0D',
        stat: 'Sum',
        period: 300,
        setPeriodToTimeRange: true,
        yAxis: {
          left: {
            label: '',
            showUnits: false,
          },
          right: {
            label: '',
            showUnits: false,
          },
        },
        legend: {
          position: 'right',
        },
        title: 'Event Type Counts',
      },
    },
    {
      type: 'metric',
      x: 0,
      y: 6,
      width: 24,
      height: 6,
      properties: {
        metrics: [
          [{ expression: "SEARCH('{AWS/Lambda,FunctionName} MetricName=\"IteratorAge\"', 'Maximum', 300)", label: '${MAX} : ${LABEL}', id: 'iterators' }],
        ],
        view: 'timeSeries',
        stacked: false,
        region: '${opt:region}',
        stat: 'Maximum',
        period: 300,
        legend: {
          position: 'right',
        },
        setPeriodToTimeRange: true,
        yAxis: {
          left: {
            label: 'ms',
            showUnits: false,
          },
          right: {
            showUnits: false,
          },
        },
        title: 'Iterator Age',
      },
    },
  ],
});

