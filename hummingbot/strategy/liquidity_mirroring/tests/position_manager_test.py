from decimal import Decimal
from hummingbot.strategy.liquidity_mirroring.position import PositionManager

if __name__=='__main__':
  pm = PositionManager()
  pm.register_trade(100, -1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "100 -1 0", "PM Incorrect Tracking"
  pm.register_trade(101, 1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "0 0 1", "PM Incorrect Tracking"

  pm = PositionManager()
  pm.register_trade(101, 1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "101 1 0", "PM Incorrect Tracking"
  pm.register_trade(100, -1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "0 0 1", "PM Incorrect Tracking"
  
  pm = PositionManager()
  pm.register_trade(100, 1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "100 1 0", "PM Incorrect Tracking"
  pm.register_trade(101, -1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "0 0 -1", "PM Incorrect Tracking"

  pm = PositionManager()
  pm.register_trade(101, -1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "101 -1 0", "PM Incorrect Tracking"
  pm.register_trade(100, 1)
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "0 0 -1", "PM Incorrect Tracking"

  pm = PositionManager()
  pm.register_trade(Decimal('421.5272727272727272727272727'), Decimal('0.2200000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5272727272727272727272727 0.2200000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('421.5291979095660077255169280'), Decimal('0.4401000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5285562793516133919103166 0.6601000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('421.5313541351105725537715844'), Decimal('0.3301000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5294889921228034740456473 0.9902000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('421.5496478073165189729606908'), Decimal('0.4401000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5356918129063832762357547 1.4303000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('421.5480157528021811572250833'), Decimal('0.3301000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5380027266530334014996591 1.7604000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('421.5485183033120278907611854'), Decimal('0.3442000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 2.1046000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('422.66000000'), Decimal('-0.00010000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 2.1045000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('422.65000000'), Decimal('-0.99010000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 1.1144000000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('422.50000000'), Decimal('-0.44004000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 0.6743600000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('427.1862020574687927932006202'), Decimal('-0.1767220000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 0.4976380000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('427.1670056119432552985349516'), Decimal('-0.3230610000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 0.1745770000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('427.1541911933080995164266760'), Decimal('-0.1484780000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "421.5397225125914663118882447 0.0260990000000000000 0", "PM Incorrect Tracking"
  pm.register_trade(Decimal('427.1541911933080995164266760'), Decimal('-0.1484780000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "427.1541911933080995164266760 -0.1223790000000000000 -5.3179282359541480892792201", "PM Incorrect Tracking"
  pm.register_trade(Decimal('429.8097589569604064159724319'), Decimal('-0.1125940000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "428.4266820615383550906733123 -0.2349730000000000000 -5.3179282359541480892792201", "PM Incorrect Tracking"
  pm.register_trade(Decimal('429.8090630769590315267942696'), Decimal('-0.0855780000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "428.7957384754558616592079894 -0.3205510000000000000 -5.3179282359541480892792201", "PM Incorrect Tracking"
  pm.register_trade(Decimal('439.2878048780487804878048780'), Decimal('-0.0410000000000000000'))
  assert f"{str(pm.avg_price)} {str(pm.amount_to_offset)} {str(pm.total_loss)}" == "429.9855421891955821190393062 -0.3615510000000000000 -5.3179282359541480892792201", "PM Incorrect Tracking"