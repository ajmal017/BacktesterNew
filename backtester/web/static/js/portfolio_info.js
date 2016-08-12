$(document).ready(function() {
    console.log( "portfolio info ready!" );
    requestPortfolioInfo();

});

function requestPortfolioInfo() {
    $.ajax({
        url: '/portfolio_info',
        success: function(portfolio_info) {
            for (var ticker in portfolio_info) {
              if (portfolio_info.hasOwnProperty(ticker)) {
                  $("#"+ticker+"-market-value-container").html(portfolio_info[ticker]['market_value'].toLocaleString());
                  $("#"+ticker+"-realized-pnl-container").html(portfolio_info[ticker]['realized_pnl'].toLocaleString());
                  $("#"+ticker+"-unrealized-pnl-container").html(portfolio_info[ticker]['unrealized_pnl'].toLocaleString());
                  $("#"+ticker+"-market-price-container").html(portfolio_info[ticker]['market_price'].toLocaleString());
                  $("#"+ticker+"-average-cost-container").html(portfolio_info[ticker]['average_cost'].toLocaleString());
                  $("#"+ticker+"-position-container").html(portfolio_info[ticker]['position'].toLocaleString());
              }
            }
            setTimeout(requestPortfolioInfo, 100);
        },
        cache: false
    });
}

//'marketValue':      'market_value',
//'realizedPNL':      'realized_pnl',
//'unrealizedPNL':    'unrealized_pnl',
//'marketPrice':      'market_price',
//'averageCost':      'average_cost',
//'position':         'position',