$(document).ready(function() {
    console.log( "account info ready!" );
    requestAccountInfo();
    //$("td.futures_pnl:contains('-')").addClass('red');
    //$("td.of_number_to_be_evaluated:contains('+')").addClass('green');
});

function requestAccountInfo() {
    $.ajax({
        url: '/account_info',
        success: function(account_info){
            //console.log(account_info);
            $("#cash-balance-container").html(account_info['cash_balance']);
            $("#available-funds-container").html(account_info['available_funds']);
            $("#buying-power-container").html(account_info['buying_power']);
            $("#excess-liquidity-container").html(account_info['excess_liquidity']);

            if(account_info['futures_pnl'] > 0){
                $("#futures-pnl-container").html(account_info['futures_pnl']).css('background-color', 'green');
            }
            else{
                $("#futures-pnl-container").html(account_info['futures_pnl']).css('background-color', 'red');
            }

            if(account_info['unrealized_pnl'] > 0){
                $("#unrealized-pnl-container").html(account_info['unrealized_pnl']).css('background-color', 'green');
            }
            else{
                $("#unrealized-pnl-container").html(account_info['unrealized_pnl']).css('background-color', 'red');
            }

            if(account_info['realized_pnl'] > 0){
                $("#realized-pnl-container").html(account_info['realized_pnl']).css('background-color', 'green');
            }
            else{
                $("#realized-pnl-container").html(account_info['realized_pnl']).css('background-color', 'red');
            }

            //$("#unrealized-pnl-container").html(account_info['unrealized_pnl']);
            //$("#realized-pnl-container").html(account_info['realized_pnl']);

            setTimeout(requestAccountInfo, 100  );
        },
        cache: false
    });
}
